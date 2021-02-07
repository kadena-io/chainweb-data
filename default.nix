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
    inherit (gargoylePkgs) gargoyle gargoyle-postgresql gargoyle-postgresql-nix gargoyle-postgresql-connect;
    pact = dontCheck super.pact;
  };
  source-overrides = {
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
