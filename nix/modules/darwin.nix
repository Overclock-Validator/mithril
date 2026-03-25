{self}: {
  config,
  lib,
  pkgs,
  ...
}: let
  cfg = config.services.mithril;
  effectiveGenerate = cfg.config.generate || cfg.environmentFile != null;
  defaultConfigDir = "/etc/mithril";
  defaultStateDir = "/var/lib/mithril";
  defaultLogsPath = "/Library/Logs/mithril";
  shared = import ./shared/lib.nix {inherit lib pkgs;};
  configDir = defaultConfigDir;
  stateDir =
    if cfg.darwin.paths.stateDir != null
    then cfg.darwin.paths.stateDir
    else defaultStateDir;
  configFile = "${configDir}/config.toml";
  configEtcPath = "${lib.removePrefix "/etc/" configDir}/config.toml";
  accountsPath =
    if cfg.darwin.paths.accountsPath != null
    then cfg.darwin.paths.accountsPath
    else "${stateDir}/accounts";
  blocksRoot =
    if cfg.darwin.paths.blocksRoot != null
    then cfg.darwin.paths.blocksRoot
    else "${stateDir}/blocks";
  fileLoggingEnabled = cfg.configSchema.logTarget == "file" || cfg.configSchema.logTarget == "both";
  logsPath =
    if cfg.darwin.paths.logsDir != null
    then cfg.darwin.paths.logsDir
    else if fileLoggingEnabled
    then defaultLogsPath
    else "${stateDir}/logs";
  configSource = shared.mkConfigToml {
    inherit cfg;
    inherit accountsPath;
    inherit blocksRoot;
    inherit logsPath;
  };
  configRunner = pkgs.writeShellScript "mithril-run" ''
    set -e

    config_template="${configFile}"
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
        configFile
      ]
      ++ cfg.extraArgs;
  darwinExternalDiskUsed = let
    pathIsExternal = p: p != null && lib.hasPrefix "/Volumes/" p;
    paths = [
      stateDir
      cfg.darwin.paths.accountsPath
      cfg.darwin.paths.blocksRoot
      cfg.darwin.paths.logsDir
      accountsPath
      blocksRoot
      logsPath
      cfg.configSchema.storageAccounts
      cfg.configSchema.storageBlockstore
      cfg.configSchema.storageSnapshots
      cfg.configSchema.storageLogs
    ];
  in
    lib.any pathIsExternal paths;
in {
  imports = [
    (import ./shared/options.nix {inherit self;})
  ];

  config = lib.mkIf cfg.enable {
    warnings =
      lib.optional (cfg.configFile != null && cfg.environmentFile != null)
      (builtins.throw "services.mithril.configFile and services.mithril.environmentFile cannot both be set.")
      ++ lib.optional (!darwinExternalDiskUsed)
      "Mithril on macOS writes heavily; use external storage (e.g. /Volumes/...) via services.mithril.darwin.paths.* or services.mithril.configSchema.storage* to reduce SSD wear.";

    services.mithril.configFile = lib.mkForce configFile;

    environment.etc."${configEtcPath}" = lib.mkIf effectiveGenerate {
      source = configSource;
    };

    launchd.daemons.mithril = {
      serviceConfig =
        {
          ProgramArguments = programArguments;
          KeepAlive = true;
          RunAtLoad = true;
          EnvironmentVariables = cfg.environment;
        }
        // lib.optionalAttrs (cfg.user != null) {
          UserName = cfg.user;
        }
        // lib.optionalAttrs (cfg.group != null) {
          GroupName = cfg.group;
        };
    };
  };
}
