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
    if builtins.hasAttr goAttr pkgs
    then pkgs.${goAttr}
    else pkgs.go;
in {
  packages = forAllSystems (
    system: let
      pkgs = import nixpkgs {inherit system;};
      mithril = pkgs.buildGoModule {
        pname = "mithril";
        version = "0.0.0";
        src = self;
        subPackages = ["cmd/mithril"];
        vendorHash = "sha256-Rslja+32Kh+q2uRuOgkHBtej6fVUh2HsVdvLFL1ytaU=";
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
