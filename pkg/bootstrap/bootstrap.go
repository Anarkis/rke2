package bootstrap

import (
	"archive/tar"
	"compress/bzip2"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/cache"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	helmv1 "github.com/k3s-io/helm-controller/pkg/apis/helm.cattle.io/v1"
	"github.com/k3s-io/helm-controller/pkg/helm"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
	"github.com/rancher/k3s/pkg/agent/util"
	"github.com/rancher/k3s/pkg/untar"
	"github.com/rancher/rke2/pkg/images"
	"github.com/rancher/wrangler/pkg/merr"
	"github.com/rancher/wrangler/pkg/schemes"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var releasePattern = regexp.MustCompile("^v[0-9]")

const bufferSize = 4096

// binDirForDigest returns the path to dataDir/data/refDigest/bin.
func binDirForDigest(dataDir string, refDigest string) string {
	return filepath.Join(dataDir, "data", refDigest, "bin")
}

// manifestsDir returns the path to dataDir/server/manifests.
func manifestsDir(dataDir string) string {
	return filepath.Join(dataDir, "server", "manifests")
}

// imagesDir returns the path to dataDir/agent/images.
func imagesDir(dataDir string) string {
	return filepath.Join(dataDir, "agent", "images")
}

// symlinkBinDir returns the path to dataDir/bin.
// This will be symlinked to the current runtime bin dir.
func symlinkBinDir(dataDir string) string {
	return filepath.Join(dataDir, "bin")
}

// runtimeCacheDir returns the path to dataDir/agent/cache/runtime
func runtimeCacheDir(dataDir string) string {
	return filepath.Join(dataDir, "agent", "cache", "runtime")
}

// dirExists returns true if a directory exists at the given path.
func dirExists(dir string) bool {
	if s, err := os.Stat(dir); err == nil && s.IsDir() {
		return true
	}
	return false
}

// Stage extracts binaries and manifests from the runtime image specified in imageConf into the directory
// at dataDir. It attempts to load the runtime image from a tarball at dataDir/agent/images,
// falling back to a remote image pull if the image is not found within a tarball.
// Extraction is skipped if a bin directory for the specified image already exists. It also rewrites
// any HelmCharts to pass through the --system-default-registry value.
// Unique image detection is accomplished by hashing the image name and tag, or the image digest,
// depending on what the runtime image reference points at.
// If the bin directory already exists, or content is successfully extracted, the bin directory path is returned.
func Stage(dataDir, privateRegistry string, resolver *images.Resolver) (string, error) {
	var img v1.Image

	ref := resolver.MustGetReference(images.Runtime)

	refDigest, err := releaseRefDigest(ref)
	if err != nil {
		return "", err
	}

	refBinDir := binDirForDigest(dataDir, refDigest)
	manifestsDir := manifestsDir(dataDir)
	cacheDir := runtimeCacheDir(dataDir)
	fsCache := cache.NewFilesystemCache(cacheDir)

	// Try to use configured runtime image from an airgap tarball
	img, err = preloadBootstrapFromRuntime(dataDir, resolver)
	if err != nil {
		return "", err
	}

	// If we didn't find the requested image in a tarball, pull it from the remote registry.
	// Note that this will fail (potentially after a long delay) if the registry cannot be reached.
	if img == nil {
		registries, err := getPrivateRegistries(privateRegistry)
		if err != nil {
			return "", errors.Wrapf(err, "failed to load private registry configuration from %s", privateRegistry)
		}
		multiKeychain := authn.NewMultiKeychain(registries, authn.DefaultKeychain)

		logrus.Infof("Pulling runtime image %s", ref)
		img, err = remote.Image(ref, remote.WithAuthFromKeychain(multiKeychain), remote.WithTransport(registries))
		if err != nil {
			return "", errors.Wrapf(err, "failed to get runtime image %s", ref)
		}
	}

	// Wrap runtime image in layer cache before extracting to speed use on subsequent boots
	img = cache.Image(img, fsCache)

	// charts get extracted every time; bin only gets extracted the first time
	extractPaths := map[string]string{
		"charts": manifestsDir,
	}

	if dirExists(refBinDir) {
		logrus.Infof("Runtime image %s bin dir already exists at %s; skipping extract", ref, refBinDir)
	} else {
		extractPaths["bin"] = refBinDir
	}

	// Extract binaries
	if err := extractToDirs(img, dataDir, extractPaths); err != nil {
		return "", errors.Wrap(err, "failed to extract runtime image")
	}

	// Ensure permissions
	if err := os.Chmod(refBinDir, 0755); err != nil {
		return "", err
	}

	// Fix up HelmCharts to pass through configured values
	// This needs to be done every time in order to sync values from the CLI
	if err := setChartValues(dataDir, resolver.Registry.Name()); err != nil {
		return "", errors.Wrap(err, "failed to set system-default-registry on HelmCharts")
	}

	// ignore errors on symlink rewrite
	_ = os.RemoveAll(symlinkBinDir(dataDir))
	_ = os.Symlink(refBinDir, symlinkBinDir(dataDir))

	return refBinDir, nil
}

// extract extracts image content to targetDir using a tar interface.
// Only files within subdirectories present in the dirs map are extracted.
func extract(targetDir string, dirs map[string]string, reader io.Reader) error {
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return err
	}

	t := tar.NewReader(reader)
	for {
		h, err := t.Next()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		targetName := filepath.Join(targetDir, h.Name)
		targetDir := filepath.Dir(h.Name)

		switch h.Typeflag {
		case tar.TypeDir:
			if _, ok := dirs[targetDir]; !ok {
				continue
			}
			if _, err := os.Stat(targetName); err != nil {
				if err := os.MkdirAll(targetName, 0755); err != nil {
					return err
				}
			}
		case tar.TypeReg:
			if _, ok := dirs[targetDir]; !ok {
				continue
			}
			logrus.Infof("Extracting file %s", h.Name)

			mode := h.FileInfo().Mode() & 0755
			f, err := os.OpenFile(targetName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
			if err != nil {
				return err
			}

			if _, err = io.Copy(f, t); err != nil {
				f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		}
	}
}

// releaseRefDigest returns a unique name for an image reference.
// If the image refers to a tag that appears to be a version string, it returns the tag + the first 12 bytes of the SHA256 hash of the reference string.
// If the image refers to a digest, it returns the digest, without the alg prefix ("sha256:", etc).
// If neither of the above conditions are met (semver tag or digest), an error is raised.
func releaseRefDigest(ref name.Reference) (string, error) {
	if t, ok := ref.(name.Tag); ok && releasePattern.MatchString(t.TagStr()) {
		hash := sha256.Sum256([]byte(ref.String()))
		return t.TagStr() + "-" + hex.EncodeToString(hash[:])[:12], nil
	} else if d, ok := ref.(name.Digest); ok {
		str := d.DigestStr()
		parts := strings.SplitN(str, ":", 2)
		if len(parts) == 2 {
			return parts[1], nil
		}
		return parts[0], nil
	}
	return "", fmt.Errorf("Runtime image %s is not a not a reference to a digest or version tag (%s)", ref, releasePattern)
}

// extractToDirs extracts to targetDir all content from img, then moves the content into place using the directory map.
// Extracted content is staged through a temporary directory and moved into place, overwriting any existing files.
func extractToDirs(img v1.Image, dataDir string, dirs map[string]string) error {
	tempDir, err := ioutil.TempDir(dataDir, "runtime-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	imageReader := mutate.Extract(img)
	defer imageReader.Close()

	// Extract image contents to temporary directory.
	if err := extract(tempDir, dirs, imageReader); err != nil {
		return err
	}

	var errs []error

	// Move the extracted content into place.
	for source, dest := range dirs {
		tempSource := filepath.Join(tempDir, source)
		if _, err := os.Stat(tempSource); err != nil {
			errs = append(errs, err)
			continue
		}

		logrus.Debugf("Moving extracted content from %s to %s", tempSource, dest)

		// Ensure the parent directory exists before renaming into the target location
		destParent := filepath.Dir(dest)
		if _, err := os.Stat(destParent); err != nil {
			if err := os.MkdirAll(destParent, 0755); err != nil {
				return err
			}
		}

		// Try to rename the temp dir into its target location.
		if err := os.Rename(tempSource, dest); err == nil {
			// Successfully renamed into place, nothing else to do.
			continue
		} else if !os.IsExist(err) {
			// Failed to rename, but not because the destination already exists.
			errs = append(errs, err)
			continue
		}

		// Target directory already exists (got ErrExist above), fall back list/rename files into place.
		files, err := ioutil.ReadDir(tempSource)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		for _, file := range files {
			src := filepath.Join(tempSource, file.Name())
			dst := filepath.Join(dest, file.Name())
			if err := os.Rename(src, dst); os.IsExist(err) {
				// Can't rename because dst already exists, remove it...
				if err = os.RemoveAll(dst); err != nil {
					errs = append(errs, errors.Wrapf(err, "failed to remove %s", dst))
					continue
				}
				// ...then try renaming again
				if err = os.Rename(src, dst); err != nil {
					errs = append(errs, errors.Wrapf(err, "failed to rename %s to %s", src, dst))
				}
			} else if err != nil {
				// Other error while renaming src to dst.
				errs = append(errs, errors.Wrapf(err, "failed to rename %s to %s", src, dst))
			}
		}
	}

	return merr.NewErrors(errs...)
}

// preloadBootstrapFromRuntime tries to load the runtime image from tarballs, using both the
// default registry, and the user-configured registry (on the off chance they've retagged the
// images in the tarball to match their private registry).
func preloadBootstrapFromRuntime(dataDir string, resolver *images.Resolver) (v1.Image, error) {
	refs := []name.Reference{resolver.MustGetReference(images.Runtime)}
	if resolver.Registry.Name() != name.DefaultRegistry {
		refs = append(refs, resolver.MustGetReference(images.Runtime, images.WithRegistry(images.DefaultRegistry)))
	}
	for _, ref := range refs {
		img, err := preloadBootstrapImage(dataDir, ref)
		if img != nil {
			return img, err
		}
		if err != nil {
			logrus.Errorf("Failed to load for bootstrap image %s: %v", ref.Name(), err)
		}
	}
	return nil, nil
}

// preloadBootstrapImage attempts return an image matching the given reference from a tarball
// within imagesDir.
func preloadBootstrapImage(dataDir string, imageRef name.Reference) (v1.Image, error) {
	imageTag, ok := imageRef.(name.Tag)
	if !ok {
		logrus.Debugf("No local image available for %s: reference is not a tag", imageRef)
		return nil, nil
	}

	imagesDir := imagesDir(dataDir)
	if _, err := os.Stat(imagesDir); err != nil {
		if os.IsNotExist(err) {
			logrus.Debugf("No local image available for %s: directory %s does not exist", imageTag, imagesDir)
			return nil, nil
		}
		return nil, err
	}

	// Walk the images dir to get a list of tar files
	files := map[string]os.FileInfo{}
	if err := filepath.Walk(imagesDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files[path] = info
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Try to find the requested tag in each file, moving on to the next if there's an error
	for fileName := range files {
		img, err := preloadFile(imageTag, fileName)
		if img != nil {
			logrus.Debugf("Found %s in %s", imageTag, fileName)
			return img, nil
		}
		if err != nil {
			logrus.Infof("Failed to check %s: %v", fileName, err)
		}
	}
	logrus.Debugf("No local image available for %s: not found in any file in %s", imageTag, imagesDir)
	return nil, nil
}

// preloadFile handles loading images from a single tarball.
func preloadFile(imageTag name.Tag, fileName string) (v1.Image, error) {
	var opener tarball.Opener
	switch {
	case util.HasSuffixI(fileName, ".txt"):
		return nil, nil
	case util.HasSuffixI(fileName, ".tar"):
		opener = func() (io.ReadCloser, error) {
			return os.Open(fileName)
		}
	case util.HasSuffixI(fileName, ".tar.lz4"):
		opener = func() (io.ReadCloser, error) {
			file, err := os.Open(fileName)
			if err != nil {
				return nil, err
			}
			zr := lz4.NewReader(file)
			return SplitReadCloser(zr, file), nil
		}
	case util.HasSuffixI(fileName, ".tar.bz2", ".tbz"):
		opener = func() (io.ReadCloser, error) {
			file, err := os.Open(fileName)
			if err != nil {
				return nil, err
			}
			zr := bzip2.NewReader(file)
			return SplitReadCloser(zr, file), nil
		}
	case util.HasSuffixI(fileName, ".tar.gz", ".tgz"):
		opener = func() (io.ReadCloser, error) {
			file, err := os.Open(fileName)
			if err != nil {
				return nil, err
			}
			zr, err := gzip.NewReader(file)
			if err != nil {
				return nil, err
			}
			return MultiReadCloser(zr, file), nil
		}
	case util.HasSuffixI(fileName, "tar.zst", ".tzst"):
		opener = func() (io.ReadCloser, error) {
			file, err := os.Open(fileName)
			if err != nil {
				return nil, err
			}
			zr, err := zstd.NewReader(file, zstd.WithDecoderMaxMemory(untar.MaxDecoderMemory))
			if err != nil {
				return nil, err
			}
			return ZstdReadCloser(zr, file), nil
		}
	default:
		return nil, errors.New("unhandled file type")
	}

	img, err := tarball.Image(opener, &imageTag)
	if err != nil {
		logrus.Debugf("Did not find %s in %s: %s", imageTag, fileName, err)
		return nil, nil
	}
	return img, nil
}

// setChartValues scans the directory at manifestDir. It attempts to load all manifests
// in that directory as HelmCharts. Any manifests that contain a HelmChart are modified to
// pass through settings to both the Helm job and the chart values.
// NOTE: This will probably fail if any manifest contains multiple documents. This should
// not matter for any of our packaged components, but may prevent this from working on user manifests.
func setChartValues(dataDir string, systemDefaultRegistry string) error {
	serializer := json.NewSerializerWithOptions(json.DefaultMetaFactory, schemes.All, schemes.All, json.SerializerOptions{Yaml: true, Pretty: true, Strict: true})
	manifestsDir := manifestsDir(dataDir)

	files := map[string]os.FileInfo{}
	if err := filepath.Walk(manifestsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		switch {
		case info.IsDir():
			return nil
		case strings.HasSuffix(path, ".yml"):
		case strings.HasSuffix(path, ".yaml"):
		default:
			return nil
		}
		files[path] = info
		return nil
	}); err != nil {
		return err
	}

	var errs []error
	for fileName, info := range files {
		if err := rewriteChart(fileName, info, dataDir, systemDefaultRegistry, serializer); err != nil {
			errs = append(errs, err)
		}
	}
	return merr.NewErrors(errs...)
}

// rewriteChart applies dataDir and systemDefaultRegistry settings to the file at fileName with associated info.
// If the file cannot be decoded as a HelmChart, it is silently skipped. Any other IO error is considered
// a failure.
func rewriteChart(fileName string, info os.FileInfo, dataDir, systemDefaultRegistry string, serializer *json.Serializer) error {
	chartChanged := false

	bytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return errors.Wrapf(err, "Failed to read manifest %s", fileName)
	}

	// Ignore manifest if it cannot be decoded
	obj, _, err := serializer.Decode(bytes, nil, nil)
	if err != nil {
		logrus.Debugf("Failed to decode manifest %s: %s", fileName, err)
		return nil
	}

	// Ignore manifest if it is not a HelmChart
	chart, ok := obj.(*helmv1.HelmChart)
	if !ok {
		logrus.Debugf("Manifest %s is %T, not HelmChart", fileName, obj)
		return nil
	}

	// Generally we should avoid using Set on HelmCharts since it cannot be overridden by HelmChartConfig,
	// but in this case we need to do it in order to avoid potentially mangling the ValuesContent field by
	// blindly appending content to it in order to set values.
	if chart.Spec.Set == nil {
		chart.Spec.Set = map[string]intstr.IntOrString{}
	}

	if chart.Spec.Set["global.rke2DataDir"].StrVal != dataDir {
		chart.Spec.Set["global.rke2DataDir"] = intstr.FromString(dataDir)
		chartChanged = true
	}

	if chart.Spec.Set["global.systemDefaultRegistry"].StrVal != systemDefaultRegistry {
		chart.Spec.Set["global.systemDefaultRegistry"] = intstr.FromString(systemDefaultRegistry)
		chartChanged = true
	}

	jobImage := helm.DefaultJobImage
	if systemDefaultRegistry != "" {
		jobImage = systemDefaultRegistry + "/" + helm.DefaultJobImage
	}

	if chart.Spec.JobImage != jobImage {
		chart.Spec.JobImage = jobImage
		chartChanged = true
	}

	if chartChanged {
		f, err := os.OpenFile(fileName, os.O_RDWR|os.O_TRUNC, info.Mode())
		if err != nil {
			return errors.Wrapf(err, "Unable to open HelmChart %s", fileName)
		}

		if err := serializer.Encode(chart, f); err != nil {
			_ = f.Close()
			return errors.Wrapf(err, "Failed to serialize modified HelmChart %s", fileName)
		}

		if err := f.Close(); err != nil {
			return errors.Wrapf(err, "Failed to write modified HelmChart %s", fileName)
		}

		logrus.Infof("Updated HelmChart %s to apply --system-default-registry modifications", fileName)
	}
	return nil
}
