package cmds

import (
	"github.com/k3s-io/k3s/pkg/cli/cmds"
	"github.com/k3s-io/k3s/pkg/cli/secretsencrypt"
	"github.com/urfave/cli"
)

func NewSecretsEncryptCommand() cli.Command {
	k3sOpts := map[string]*K3SFlagOption{
		"data-dir": copy,
		"token":    copy,
		"server": {
			Default: "https://127.0.0.1:9345",
		},
		"f":      ignore,
		"skip":   ignore,
		"output": ignore,
	}
	command := cmds.NewSecretsEncryptCommands(
		secretsencrypt.Status,
		secretsencrypt.Enable,
		secretsencrypt.Disable,
		secretsencrypt.Prepare,
		secretsencrypt.Rotate,
		secretsencrypt.Reencrypt)

	for i, subcommand := range command.Subcommands {
		command.Subcommands[i] = mustCmdFromK3S(subcommand, k3sOpts)
	}
	return mustCmdFromK3S(command, nil)
}
