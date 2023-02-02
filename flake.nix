{
  description = "Data ingestion for Chainweb";

  inputs = {
    nixpkgs.follows = "haskellNix/nixpkgs-unstable";
    haskellNix.url = "github:input-output-hk/haskell.nix";
    flake-utils.url = "github:numtide/flake-utils";
  };

  nixConfig = {
    # This sets the flake to use the IOG nix cache.
    # Nix should ask for permission before using it,
    # but remove it here if you do not want it to.
    extra-substituters = ["https://cache.iog.io"];
    extra-trusted-public-keys = ["hydra.iohk.io:f/Ea+s+dFdN+3Y/G+FDgSq+a5NEWhJGzdjvKNGv0/EQ="];
    allow-import-from-derivation = "true";
  };

  outputs = { self, nixpkgs, flake-utils, haskellNix }:
    flake-utils.lib.eachSystem
      [ "x86_64-linux" "x86_64-darwin"
        "aarch64-linux" "aarch64-darwin" ] (system:
        let
          pkgs = import nixpkgs {
            inherit system overlays;
            inherit (haskellNix) config;
          };
          flake = pkgs.chainweb-data.flake {

          };
          executable = (flake.packages."chainweb-data:exe:chainweb-data").override { dontStrip = false; };
          overlays = [ haskellNix.overlay
            (final: prev: {
	    chainweb-data =
	      final.haskell-nix.project' {
	        src = ./.;
	        compiler-nix-name = "ghc8107";
                projectFileName = "cabal.project";
	        shell.tools = {
	          cabal = {};
	        };
	      };

            })
          ];
        in  flake // {
          packages.default = executable; 
          packages.chainweb-data-docker = import ./docker.nix { inherit pkgs; chainweb-data = executable; };
          });
}
