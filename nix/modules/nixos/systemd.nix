{
  config,
  lib,
  pkgs,
  ...
}: let
  cfg = config.services.mithril;
  base = import ../shared/systemd-base.nix {inherit config lib pkgs;};
  baseUnit = {
    unitConfig = base.baseUnit.unitConfig or {};
    serviceConfig = base.baseUnit.serviceConfig or {};
  };
in {
  config = lib.mkIf (cfg.enable && !pkgs.stdenv.isDarwin) {
    systemd.services.mithril =
      baseUnit
      // {
        serviceConfig =
          baseUnit.serviceConfig
          // {
            DynamicUser = true;
          };
        wantedBy = ["multi-user.target"];
      };
  };
}
