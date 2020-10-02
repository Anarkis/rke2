# Advanced Options and Configuration

This section contains advanced information describing the different ways you can run and manage RKE2:

- [Certificate rotation](#certificate-rotation)
- [Auto-deploying manifests](#auto-deploying-manifests)
- [Configuring containerd](#configuring-containerd)
- [Secrets Encryption Config](#secrets-encryption-config-experimental)
- [Node labels and taints](#node-labels-and-taints)
- [Starting the server with the installation script](#starting-the-server-with-the-installation-script)

# Certificate Rotation

By default, certificates in RKE2 expire in 12 months.

If the certificates are expired or have fewer than 90 days remaining before they expire, the certificates are rotated when RKE2 is restarted.

# Auto-Deploying Manifests

Any file found in `/var/lib/rancher/rke2/server/manifests` will automatically be deployed to Kubernetes in a manner similar to `kubectl apply`.

For information about deploying Helm charts using the manifests directory, refer to the section about [Helm.](helm.md)

# Configuring containerd

RKE2 will generate config.toml for containerd in `/var/lib/rancher/rke2/agent/etc/containerd/config.toml`.

For advanced customization for this file you can create another file called `config.toml.tmpl` in the same directory and it will be used instead.

The `config.toml.tmpl` will be treated as a Go template file, and the `config.Node` structure is being passed to the template. [This template](https://github.com/rancher/k3s/blob/master/pkg/agent/templates/templates.go#L16-L32) example on how to use the structure to customize the configuration file.

# Secrets Encryption Config
RKE2 supports the secrets encryption at rest, and will do the following automatically:

- Generate an AES-CBC key
- Generate an encryption config file with the generated key

```
{
  "kind": "EncryptionConfiguration",
  "apiVersion": "apiserver.config.k8s.io/v1",
  "resources": [
    {
      "resources": [
        "secrets"
      ],
      "providers": [
        {
          "aescbc": {
            "keys": [
              {
                "name": "aescbckey",
                "secret": "xxxxxxxxxxxxxxxxxxx"
              }
            ]
          }
        },
        {
          "identity": {}
        }
      ]
    }
  ]
}
```

- Pass the config to the Kubernetes APIServer as encryption-provider-config

Once enabled any created secret will be encrypted with this key. Note that if you disable encryption then any encrypted secrets will not be readable until you enable encryption again.

# Node Labels and Taints

RKE2 agents can be configured with the options `--node-label` and `--node-taint` which adds a label and taint to the kubelet. The two options only add labels and/or taints [at registration time,](FIXME/rke2/latest/en/installation/install-options/#node-labels-and-taints-for-agents) so they can only be added once and not changed after that again by running RKE2 commands.

If you want to change node labels and taints after node registration you should use `kubectl`. Refer to the official Kubernetes documentation for details on how to add [taints](https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/) and [node labels.](https://kubernetes.io/docs/tasks/configure-pod-container/assign-pods-nodes/#add-a-label-to-a-node)

# Starting the Server with the Installation Script

The installation script provides units for systemd, but does not enable or start the service by default.

When running with openrc, logs will be created at `/var/log/rke2.log`. 

When running with systemd, logs will be created in `/var/log/syslog` and viewed using `journalctl -u rke2-server` or `journalctl -u rke2-agent`.

An example of installing with the install script:

```bash
curl -sfL https://get.rke2.io | sh -
systemctl enable rke2-server
systemctl start rke2-server
```
