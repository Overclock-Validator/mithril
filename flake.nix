{
  description = "Mithril devshell and Nix modules";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = {
    self,
    nixpkgs,
  }: let
    packages = import ./nix/packages.nix {inherit self nixpkgs;};
    devshells = import ./nix/devshells.nix {inherit self nixpkgs packages;};
    nixosModule = import ./nix/modules/nixos {inherit self;};
    darwinModule = import ./nix/modules/darwin.nix {inherit self;};
    homeManagerModule = import ./nix/modules/home-manager {inherit self;};
    formatter = packages.forAllSystems (
      system: let
        pkgs = import nixpkgs {inherit system;};
      in
        pkgs.alejandra
    );
  in {
    inherit (packages) packages;
    inherit (devshells) devShells;
    inherit formatter;

    checks = packages.forAllSystems (system: let
      pkgs = import nixpkgs {inherit system;};
      inherit (pkgs) lib;
      nixpkgsLib = nixpkgs.lib;
      shared = import ./nix/modules/shared/lib.nix {inherit lib pkgs;};
      fakeHmBase = {lib, ...}: {
        options = {
          warnings = lib.mkOption {
            type = lib.types.listOf lib.types.str;
            default = [];
          };
          xdg = {
            configHome = lib.mkOption {
              type = lib.types.str;
              default = "/tmp/xdg-config";
            };
            stateHome = lib.mkOption {
              type = lib.types.str;
              default = "/tmp/xdg-state";
            };
            configFile = lib.mkOption {
              type = lib.types.attrs;
              default = {};
            };
          };
          home.file = lib.mkOption {
            type = lib.types.attrs;
            default = {};
          };
          systemd.user.services = lib.mkOption {
            type = lib.types.attrs;
            default = {};
          };
          launchd.agents = lib.mkOption {
            type = lib.types.attrs;
            default = {};
          };
        };
      };
      fakeDarwinBase = {lib, ...}: {
        options = {
          warnings = lib.mkOption {
            type = lib.types.listOf lib.types.str;
            default = [];
          };
          environment.etc = lib.mkOption {
            type = lib.types.attrs;
            default = {};
          };
          launchd.daemons = lib.mkOption {
            type = lib.types.attrs;
            default = {};
          };
          system.activationScripts = lib.mkOption {
            type = lib.types.attrs;
            default = {};
          };
        };
      };
      nixosEval =
        if pkgs.stdenv.isLinux
        then
          nixpkgsLib.nixosSystem {
            inherit system;
            modules = [
              self.nixosModules.mithril
              {
                services.mithril.enable = true;
                services.mithril.config.generate = false;
              }
            ];
          }
        else null;
      hmEval = lib.evalModules {
        modules = [
          fakeHmBase
          self.lib.homeManagerModules.mithril
          {
            services.mithril.enable = true;
            services.mithril.config.generate = false;
          }
        ];
        specialArgs = {inherit pkgs;};
      };
      darwinEval = lib.evalModules {
        modules = [
          fakeDarwinBase
          self.darwinModules.mithril
          {
            services.mithril.enable = true;
            services.mithril.config.generate = false;
          }
        ];
        specialArgs = {inherit pkgs;};
      };
      configSmoke = shared.mkConfigToml {
        cfg = hmEval.config.services.mithril;
        accountsPath = "/tmp/mithril/accounts";
        blocksRoot = "/tmp/mithril/blocks";
        logsPath = "/tmp/mithril/logs";
      };
    in
      {
        nixfmt = pkgs.runCommand "nixfmt-check" {nativeBuildInputs = [pkgs.alejandra];} ''
          alejandra --check ${self}
          touch $out
        '';
        deadnix = pkgs.runCommand "deadnix-check" {nativeBuildInputs = [pkgs.deadnix];} ''
          deadnix --fail ${self}
          touch $out
        '';
        statix = pkgs.runCommand "statix-check" {nativeBuildInputs = [pkgs.statix];} ''
          statix check ${self}
          touch $out
        '';
        home-manager-module = assert pkgs.stdenv.isDarwin || hmEval.config.systemd.user.services.mithril.serviceConfig.LimitNOFILE == 65536;
          pkgs.runCommand "home-manager-module-eval" {} ''
            echo ${lib.escapeShellArg (builtins.toString hmEval.config.services.mithril.package)} > /dev/null
            touch $out
          '';
        darwin-module = pkgs.runCommand "darwin-module-eval" {} ''
          echo ${lib.escapeShellArg (builtins.toString darwinEval.config.services.mithril.package)} > /dev/null
          touch $out
        '';
        config-smoke = pkgs.runCommand "mithril-config-smoke" {} ''
                grep -F 'cluster = "alpenglow"' ${configSmoke} >/dev/null
                grep -F 'mode = "verifying"' ${configSmoke} >/dev/null
                grep -F 'https://rpc.ag.validator1.net' ${configSmoke} >/dev/null
                grep -F 'working_set_max_mb = 1024' ${configSmoke} >/dev/null
          grep -F 'enforce_disk_reserve = true' ${configSmoke} >/dev/null
          grep -F 'max_source_mb = 64' ${configSmoke} >/dev/null
                cp ${configSmoke} $out
        '';
      }
      // lib.optionalAttrs pkgs.stdenv.isLinux {
        nixos-module = assert nixosEval.config.systemd.services.mithril.serviceConfig.LimitNOFILE == 65536;
          pkgs.runCommand "nixos-module-eval" {} ''
            echo ${lib.escapeShellArg (builtins.toString nixosEval.config.services.mithril.package)} > /dev/null
            touch $out
          '';
      });

    nixosModules = {
      mithril = nixosModule;
      default = nixosModule;
    };

    darwinModules = {
      mithril = darwinModule;
      default = darwinModule;
    };

    lib = {
      homeManagerModules = {
        mithril = homeManagerModule;
        default = homeManagerModule;
      };
    };
  };
}
