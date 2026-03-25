{
  config,
  lib,
  pkgs,
  ...
}: let
  cfg = config.services.mithril;
  effectiveGenerate = cfg.config.generate || cfg.environmentFile != null;
  dirName = "mithril";
  configDir = "${config.xdg.configHome}/${dirName}";
  stateDir =
    if pkgs.stdenv.isDarwin && cfg.darwin.paths.stateDir != null
    then cfg.darwin.paths.stateDir
    else "${config.xdg.stateHome}/${dirName}";
  accountsPath =
    if pkgs.stdenv.isDarwin && cfg.darwin.paths.accountsPath != null
    then cfg.darwin.paths.accountsPath
    else "${stateDir}/accounts";
  blocksRoot =
    if pkgs.stdenv.isDarwin && cfg.darwin.paths.blocksRoot != null
    then cfg.darwin.paths.blocksRoot
    else "${stateDir}/blocks";
  fileLoggingEnabled = cfg.configSchema.logTarget == "file" || cfg.configSchema.logTarget == "both";
  logsPath =
    if !fileLoggingEnabled
    then null
    else if pkgs.stdenv.isDarwin && cfg.darwin.paths.logsDir != null
    then cfg.darwin.paths.logsDir
    else cfg.configSchema.storageLogs;
  shared = import ../shared/lib.nix {inherit lib pkgs;};
  configTemplate = shared.mkConfigTomlTemplate {
    inherit cfg;
    accountsPath = "@STATE_DIRECTORY@/accounts";
    blocksRoot = "@STATE_DIRECTORY@/blocks";
    logsPath =
      if fileLoggingEnabled
      then "@LOGS_DIRECTORY@"
      else "@STATE_DIRECTORY@/logs";
  };
  configInitScript = pkgs.writeShellScript "mithril-generate-config-user" ''
    set -euo pipefail
    config_dir="$CONFIGURATION_DIRECTORY"
    state_dir="$STATE_DIRECTORY"
    logs_dir="''${LOGS_DIRECTORY:-}"
    runtime_dir="$RUNTIME_DIRECTORY"
    mkdir -p "$config_dir"
    mkdir -p "$runtime_dir"
    cat > "$config_dir/config.toml" <<'EOF'
    ${configTemplate}
    EOF
    sed -i "s|@STATE_DIRECTORY@|$state_dir|g" "$config_dir/config.toml"
    sed -i "s|@LOGS_DIRECTORY@|$logs_dir|g" "$config_dir/config.toml"
    awk '
      function esc(s, t) {
        t = s
        gsub(/\\/,"\\\\", t)
        gsub(/&/,"\\\\&", t)
        return t
      }
      {
        line = $0
        for (name in ENVIRON) {
          pattern = "\\$" name
          gsub(pattern, esc(ENVIRON[name]), line)
          pattern = "\\$\\{" name "\\}"
          gsub(pattern, esc(ENVIRON[name]), line)
        }
        print line
      }
    ' "$config_dir/config.toml" > "$runtime_dir/config.toml.tmp"
    mv "$runtime_dir/config.toml.tmp" "$config_dir/config.toml"
  '';
  configPath =
    if cfg.configFile != null
    then cfg.configFile
    else "${configDir}/config.toml";
  configRunner = pkgs.writeShellScript "mithril-run-user" ''
    set -e

    config_template="${configPath}"
    env_file="${cfg.environmentFile or ""}"

    if [ -n "$env_file" ] && [ -f "$env_file" ]; then
      set -a
      . "$env_file"
      set +a
    fi

    tmp_config="$("${pkgs.coreutils}/bin/mktemp" -t mithril-config.XXXXXX)"
    "${pkgs.gawk}/bin/awk" '
      function esc(s, t) {
        t = s
        gsub(/\\/,"\\\\", t)
        gsub(/&/,"\\\\&", t)
        return t
      }
      {
        line = $0
        for (name in ENVIRON) {
          pattern = "\\$" name
          gsub(pattern, esc(ENVIRON[name]), line)
          pattern = "\\$\\{" name "\\}"
          gsub(pattern, esc(ENVIRON[name]), line)
        }
        print line
      }
    ' "$config_template" > "$tmp_config"

    exec "${cfg.package}/bin/mithril" run --config "$tmp_config" ${lib.escapeShellArgs cfg.extraArgs}
  '';
  programArguments =
    if effectiveGenerate
    then [configRunner]
    else
      [
        "${cfg.package}/bin/mithril"
        "run"
        "--config"
        configPath
      ]
      ++ cfg.extraArgs;
  inherit (cfg) environment;
  darwinExternalDiskUsed = let
    pathIsExternal = p: p != null && lib.hasPrefix "/Volumes/" p;
    paths = [
      cfg.darwin.paths.stateDir
      cfg.darwin.paths.accountsPath
      cfg.darwin.paths.blocksRoot
      cfg.darwin.paths.logsDir
      cfg.configSchema.storageAccounts
      cfg.configSchema.storageBlockstore
      cfg.configSchema.storageSnapshots
      cfg.configSchema.storageLogs
    ];
  in
    lib.any pathIsExternal paths;
in {
  config = lib.mkIf cfg.enable (
    lib.mkMerge [
      {
        warnings =
          lib.optional (cfg.configFile != null && cfg.environmentFile != null)
          (builtins.throw "services.mithril.configFile and services.mithril.environmentFile cannot both be set.");
      }
      (lib.mkIf (effectiveGenerate && pkgs.stdenv.isDarwin) {
        xdg.configFile."mithril/config.toml".source = shared.mkConfigToml {
          inherit cfg;
          inherit accountsPath;
          inherit blocksRoot;
          inherit logsPath;
        };
      })
      (lib.mkIf (!pkgs.stdenv.isDarwin && effectiveGenerate && cfg.configFile == null) {
        systemd.user.services.mithril = {
          path = [pkgs.coreutils pkgs.gnused pkgs.gawk];
          serviceConfig.ExecStartPre = [configInitScript];
        };
      })
      (lib.mkIf pkgs.stdenv.isDarwin {
        warnings =
          lib.optional (!darwinExternalDiskUsed)
          "Mithril on macOS writes heavily; use external storage (e.g. /Volumes/...) via services.mithril.darwin.paths.* or services.mithril.configSchema.storage* to reduce SSD wear.";
        launchd.agents.mithril = {
          serviceConfig = {
            ProgramArguments = programArguments;
            KeepAlive = true;
            RunAtLoad = true;
            EnvironmentVariables = environment;
          };
        };
      })
    ]
  );
}
