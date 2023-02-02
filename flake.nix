{
  description = "Data ingestion for Chainweb";

  inputs = {
    # nixpkgs.url = "github:NixOS/nixpkgs?rev=7a94fcdda304d143f9a40006c033d7e190311b54";
    # nixpkgs.url = "github:NixOS/nixpkgs?rev=4d2b37a84fad1091b9de401eb450aae66f1a741e";
    haskellNix.url = "github:input-output-hk/haskell.nix";
    nixpkgs.follows = "haskellNix/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
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
          overlays = [ haskellNix.overlay
            (final: prev: {
	    chainweb-data =
	      final.haskell-nix.project' {
	        src = ./.;
	        compiler-nix-name = "ghc8107";
                cabalProject = "cabal.project";
	        shell.tools = {
	          cabal = {};
	        };
	        shell.buildInputs = with pkgs; [

	        ];
	      };

            })
          ];
        in flake // {
          packages.default = flake.packages."chainweb-data:exe:chainweb-data";
          });
}
