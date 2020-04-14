{ kpkgs ? import ./deps/kpkgs {}
}:
let pkgs = kpkgs.pkgs;
    haskellPackages = kpkgs.rp.ghc8_6;

in haskellPackages.developPackage {
  name = builtins.baseNameOf ./.;
  root = kpkgs.gitignoreSource ./.;
  overrides = self: super: with pkgs.haskell.lib; {
    chainweb-api = self.callCabal2nix "chainweb-api" (pkgs.hackGet ./deps/chainweb-api) {};
  };
#  source-overrides = {
#    chainweb-api = pkgs.fetchFromGitHub {
#      owner = "kadena-io";
#      repo = "chainweb-api";
#      rev = "ed82f95111395313476d988a8c531d669593b034";
#      sha256 = "06mzlbdbpc86y2ncznvbqipj78l54lsaw5pnrfjz2v4ljn6n92jc";
#    };
#  };
  modifier = drv: pkgs.haskell.lib.overrideCabal drv (attrs: {
    buildTools = (attrs.buildTools or []) ++ [
      haskellPackages.cabal-install
      haskellPackages.ghcid
    ];
  });
}
