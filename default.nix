{ kpkgs ? import ./deps/kpkgs {}
}:
let pkgs = kpkgs.pkgs;
    haskellPackages = kpkgs.rp.ghc8_6;
    nix-thunk = import ./deps/nix-thunk {};
in haskellPackages.developPackage {
  name = builtins.baseNameOf ./.;
  root = kpkgs.gitignoreSource ./.;
  overrides = self: super: with pkgs.haskell.lib;
  let gargoylePkgs = import ./deps/gargoyle { haskellPackages = self; };
  in
  {
    inherit (gargoylePkgs) gargoyle gargoyle-postgresql;

    #beam-automigrate = self.callHackageDirect {
    #  pkg = "beam-automigrate";
    #  ver = "0.1.2.0";
    #  sha256 = "1a70da15hb4nlpxhnsy1g89frbpf3kg3mwb4g9carj5izw1w1r1k";
    #} {};

    direct-sqlite = dontCheck (self.callHackageDirect {
      pkg = "direct-sqlite";
      ver = "2.3.26";
      sha256 = "1kdkisj534nv5r0m3gmxy2iqgsn6y1dd881x77a87ynkx1glxfva";
    } {});

    pact = dontCheck super.pact;

    pact-time = dontCheck (self.callHackageDirect {
      pkg = "pact-time";
      ver = "0.2.0.0";
      sha256 = "1cfn74j6dr4279bil9k0n1wff074sdlz6g1haqyyy38wm5mdd7mr";
    } {});
  };
  source-overrides = {
    beam-automigrate = nix-thunk.thunkSource ./deps/beam-automigrate;
    chainweb-api = nix-thunk.thunkSource ./deps/chainweb-api;
    pact = nix-thunk.thunkSource ./deps/pact;
  };
  modifier = drv: pkgs.haskell.lib.overrideCabal drv (attrs: {
    buildTools = (attrs.buildTools or []) ++ [
      haskellPackages.cabal-install
      haskellPackages.ghcid
    ];
  });
}
