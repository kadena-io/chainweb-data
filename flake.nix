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
    extra-substituters = ["https://cache.iog.io" "https://nixcache.chainweb.com"];
    extra-trusted-public-keys = ["hydra.iohk.io:f/Ea+s+dFdN+3Y/G+FDgSq+a5NEWhJGzdjvKNGv0/EQ=" "nixcache.chainweb.com:FVN503ABX9F8x8K0ptnc99XEz5SaA4Sks6kNcZn2pBY="];
    allow-import-from-derivation = "true";
  };

  outputs = { self, nixpkgs, flake-utils, haskellNix }:
    flake-utils.lib.eachSystem
      [ "x86_64-linux" "x86_64-darwin"
        "aarch64-linux" "aarch64-darwin" ] (system:
        let
          pkgs = import nixpkgs {
            inherit system;
            inherit (haskellNix) config;
            overlays = [ haskellNix.overlay ];
          };
          defaultNix = import ./default.nix { inherit pkgs; };
          flake = defaultNix.flake;
          executable = defaultNix.default;
        in  flake // {
          packages.default = executable;
          packages.chainweb-data-docker = defaultNix.dockerImage;
          nixosModules.default = nix/nixos-module.nix;
        });
}
