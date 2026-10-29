{
  self,
  nixpkgs,
}: let
  systems = [
    "x86_64-linux"
    "aarch64-linux"
    "x86_64-darwin"
    "aarch64-darwin"
  ];
  forAllSystems = f: nixpkgs.lib.genAttrs systems f;
  goMod = builtins.readFile (self + "/go.mod");
  goModLines = nixpkgs.lib.splitString "\n" goMod;
  toolchainLine =
    nixpkgs.lib.findFirst (line: nixpkgs.lib.hasPrefix "toolchain go" line) null goModLines;
  goLine = nixpkgs.lib.findFirst (line: nixpkgs.lib.hasPrefix "go " line) null goModLines;
  versionLine =
    if toolchainLine != null
    then toolchainLine
    else goLine;
  versionString =
    if versionLine == null
    then null
    else if nixpkgs.lib.hasPrefix "toolchain go" versionLine
    then nixpkgs.lib.removePrefix "toolchain go" versionLine
    else nixpkgs.lib.removePrefix "go " versionLine;
  goVersion =
    if versionString == null
    then null
    else nixpkgs.lib.splitString "." versionString;
  goAttr =
    if goVersion == null || (builtins.length goVersion) < 2
    then "go"
    else if builtins.elemAt goVersion 0 == "1"
    then "go_1_${builtins.elemAt goVersion 1}"
    else "go_${builtins.elemAt goVersion 0}_${builtins.elemAt goVersion 1}";
  goFor = pkgs:
    assert versionString == "1.27.2";
    (
      if builtins.hasAttr goAttr pkgs
      then pkgs.${goAttr}
      else pkgs.go
    ).overrideAttrs (_: {
      version = "1.27.2";
      src = pkgs.fetchurl {
        url = "https://go.dev/dl/go1.27.2.src.tar.gz";
        hash = "sha256-A0ldorpkiU1A9cSZLklFT6eLUGkGBP+Stq//UIG3bmI=";
      };
    });
in {
  packages = forAllSystems (
    system: let
      pkgs = import nixpkgs {inherit system;};
      go = goFor pkgs;
      buildGoModule = pkgs.buildGoModule.override {inherit go;};
      mithril = buildGoModule {
        pname = "mithril";
        version = "0.0.0";
        src = self;
        subPackages = [
          "cmd/mithril"
        ];
        vendorHash = "sha256-M8n72lWRa1QwY5virSIGQxa1QGSYogYfD0F3lCsuclk=";
        nativeBuildInputs = [pkgs.pkg-config];
        buildInputs = [pkgs.zstd];
        env = {
          CGO_ENABLED = "1";
        };
        # Work around missing Go field asm includes in Nix Go toolchains.
        tags = ["purego"];
        ldflags = [
          "-s"
          "-w"
        ];
      };
    in {
      inherit mithril;
      default = mithril;
    }
  );

  inherit goFor;
  inherit systems;
  inherit forAllSystems;
}
