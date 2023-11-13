{
  description = "Data ingestion for Chainweb";

  inputs = {
    hs-nix-infra.url = "github:kadena-io/hs-nix-infra/enis/metadata-experiments";
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

  outputs = { self, hs-nix-infra, flake-utils }:
    flake-utils.lib.eachSystem
      [ "x86_64-linux" "x86_64-darwin"
        "aarch64-linux" "aarch64-darwin" ] (system:
        let
          inherit (hs-nix-infra) nixpkgs haskellNix;
          pkgs = import nixpkgs {
            inherit system;
            inherit (haskellNix) config;
            overlays = [ haskellNix.overlay ];
          };
          defaultNix = import ./default.nix { inherit pkgs; };
          flake = defaultNix.flake;
          executable = defaultNix.default;
          # This package depends on other packages at buildtime, but its output does not
          # depend on them. This way, we don't have to download the entire closure to verify
          # that those packages build.
          mkCheck = name: package: pkgs.runCommand ("check-"+name) {} ''
            echo ${name}: ${package}
            echo works > $out
          '';
        in {
          packages = {
            default = executable;

            recursive = with hs-nix-infra.lib.recursive system;
              wrapRecursiveWithMeta "chainweb-data" "${wrapFlake self}.default";

            chainweb-data-docker = defaultNix.dockerImage;

            # Built by CI
            check = pkgs.runCommand "check" {} ''
              echo ${self.packages.${system}.default}
              echo ${mkCheck "devShell" flake.devShell}
              echo works > $out
            '';
          };
          devShell = flake.devShell;

          # Expose the haskellNix project
          project = defaultNix.project;
        });
}
