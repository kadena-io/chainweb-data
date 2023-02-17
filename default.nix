let inputs = import ./flake-inputs.nix;
    pkgsDef = import inputs.nixpkgs (import inputs.haskellNix {}).nixpkgsArgs;
in
{ pkgs ? pkgsDef
, ...
}:
let chainweb-data = pkgs.haskell-nix.project' {
	    src = ./.;
      index-state = "2023-02-01T00:00:00Z";
	    compiler-nix-name = "ghc8107";
      projectFileName = "cabal.project";
	    shell.tools = {
	      cabal = {};
	    };
    };
    flake = chainweb-data.flake {};
    default = flake.packages."chainweb-data:exe:chainweb-data".override { dontStrip = false; };
in {
  inherit flake default;
}