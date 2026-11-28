# Nix Usage

This repository ships a `flake.nix` with:

- `packages.mithril` (builds the `mithril` binary)
- `devShells.default` (Go + build tools)
- `nixosModules.mithril`
- `darwinModules.mithril`
- `lib.homeManagerModules.mithril`

The module can generate `config.toml` from Nix options. You can still override
or add any config keys via `services.mithril.config.settings`.

---

## Flake Usage (NixOS)

```nix
{
  inputs.mithril.url = "path:/path/to/mithril";

  outputs = { self, nixpkgs, mithril, ... }:
    {
      nixosConfigurations.server = nixpkgs.lib.nixosSystem {
        system = "x86_64-linux";
        modules = [
          mithril.nixosModules.mithril
          ({ config, ... }: {
            services.mithril = {
              enable = true;

              # Storage (single disk example)
              storage.singleDisk = {
                enable = true;
                device = "/dev/disk/by-id/nvme-FAST...";
                fsType = "ext4";
                format = {
                  enable = true;
                  force = true;  // destructive
                };
              };

              # Config schema (typed)
              configSchema = {
                name = "mithril";
                networkCluster = "mainnet";
                networkRpc = [ "https://api.mainnet-beta.solana.com" ];
                blockSource = "rpc";
              };

              # Performance tuning
              performance.enable = true;

              # Optional EnvironmentFile (systemd) and interpolation in config
              environmentFile = "/etc/mithril/mithril.env";
              config.settings = {
                log = { level = "$MITHRIL_LOG_LEVEL"; };
              };

              # Firewall (NixOS module)
              openFirewall = true;
            };
          })
        ];
      };
    };
}
```

---

## Flake Usage (nix-darwin)

```nix
{
  inputs.mithril.url = "path:/path/to/mithril";

  outputs = { self, nixpkgs, mithril, ... }:
    {
      darwinConfigurations.macos = nixpkgs.lib.darwinSystem {
        system = "aarch64-darwin";
        modules = [
          mithril.darwinModules.mithril
          ({ config, ... }: {
            services.mithril = {
              enable = true;

              # Config generation (system paths)
              configSchema = {
                name = "mithril";
                networkCluster = "mainnet";
                networkRpc = [ "https://api.mainnet-beta.solana.com" ];
              };

              # Optional user/group for launchd daemon
              # user = "mithril";
              # group = "staff";
            };
          })
        ];
      };
    };
}
```

---

## Home Manager (Linux or macOS)

```nix
{
  inputs.mithril.url = "path:/path/to/mithril";

  outputs = { self, nixpkgs, mithril, ... }:
    {
      homeConfigurations.user = nixpkgs.lib.homeManagerConfiguration {
        pkgs = import nixpkgs { system = "x86_64-linux"; };
        modules = [
          mithril.lib.homeManagerModules.mithril
          ({ config, ... }: {
            services.mithril = {
              enable = true;

              # Generates ${XDG_CONFIG_HOME}/mithril/config.toml
              configSchema = {
                name = "mithril";
                networkCluster = "mainnet";
                networkRpc = [ "https://api.mainnet-beta.solana.com" ];
              };
            };
          })
        ];
      };
    };
}
```

---

## Notes

- Disk formatting/mounting is only available in the NixOS module.
- `config.settings` merges last and overrides any typed option.
- On Linux systemd units, `services.mithril.environmentFile` loads environment
  variables at runtime and interpolates `$ENV_VAR` or `${ENV_VAR}` in the
  generated `config.toml`.
- On macOS (nix-darwin or Home Manager launchd), `services.mithril.environmentFile`
  is sourced at runtime before interpolation; ensure the file is shell-compatible.
- The module generates `config.toml` by default, and always generates when
  `services.mithril.environmentFile` is set:
  - NixOS/nix-darwin: `/etc/mithril/config.toml` (fixed on nix-darwin)
  - Home Manager: `${XDG_CONFIG_HOME}/mithril/config.toml`
- On macOS, prefer external storage for Mithril data. You can override paths via:
  - `services.mithril.darwin.paths.stateDir`
  - `services.mithril.darwin.paths.accountsPath`
  - `services.mithril.darwin.paths.blocksRoot`
  - `services.mithril.darwin.paths.logsDir`
