{
  config,
  lib,
  pkgs,
  ...
}: let
  cfg = config.services.mithril;
  environmentList = lib.mapAttrsToList (name: value: "${name}=${value}") cfg.environment;
  configPath =
    if cfg.configFile != null
    then cfg.configFile
    else "$CONFIGURATION_DIRECTORY/config.toml";
  execStart =
    if cfg.configFile != null
    then
      lib.escapeShellArgs [
        "${cfg.package}/bin/mithril"
        "run"
        "--config"
        cfg.configFile
      ]
    else
      lib.escapeShellArgs [
        pkgs.runtimeShell
        "-c"
        "${cfg.package}/bin/mithril run --config \"${configPath}\""
      ];
  fileLoggingEnabled = cfg.configSchema.logTarget == "file" || cfg.configSchema.logTarget == "both";
  rwPaths =
    lib.filter (p: p != null) (
      if cfg.storage.singleDisk.enable
      then [cfg.storage.singleDisk.mountPoint]
      else [
        cfg.storage.accounts.mountPoint
        cfg.storage.blocks.mountPoint
      ]
    )
    ++ lib.optional (fileLoggingEnabled && cfg.configSchema.storageLogs != null) cfg.configSchema.storageLogs;
in {
  baseUnit = {
    unitConfig = {
      Description = "Mithril full node";
      After = ["network-online.target"];
      Wants = ["network-online.target"];
    };
    serviceConfig =
      {
        ExecStart = execStart;
        Restart = "on-failure";
        RestartSec = "5s";
        Environment = environmentList;
        WorkingDirectory = "%S/mithril";
        ConfigurationDirectory = "mithril";
        StateDirectory = "mithril";
        CacheDirectory = "mithril";
        RuntimeDirectory = "mithril";
        ReadWritePaths = rwPaths;
      }
      // lib.optionalAttrs fileLoggingEnabled {
        LogsDirectory = "mithril";
      }
      // lib.optionalAttrs (cfg.environmentFile != null) {
        EnvironmentFile = [cfg.environmentFile];
      };
  };
}
