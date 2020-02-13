{ compiler ? "ghc865"
, rev      ? "d5291756487d70bc336e33512a9baf9fa1788faf"
, sha256   ? "0mhqhq21y5vrr1f30qd2bvydv4bbbslvyzclhw0kdxmkgg3z4c92"
, pkgs     ?
    import (builtins.fetchTarball {
      url    = "https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz";
      inherit sha256; }) {
      config.allowBroken = false;
      config.allowUnfree = true;
    }
}:
let gitignoreSrc = pkgs.fetchFromGitHub {
      owner = "hercules-ci";
      repo = "gitignore";
      rev = "f9e996052b5af4032fe6150bba4a6fe4f7b9d698";
      sha256 = "0jrh5ghisaqdd0vldbywags20m2cxpkbbk5jjjmwaw0gr8nhsafv";
    };
    inherit (import gitignoreSrc { inherit (pkgs) lib; }) gitignoreSource;

    beam-src = pkgs.fetchFromGitHub {
      owner = "kadena-io";
      repo = "beam";
      rev = "7e7f7182f01959a9768d751694a47c593c125373";
      sha256 = "098sdfhyni4brsv3296anygbxiyl13n6k4xvx0chjra9hph8gvy0";
    };

in
pkgs.haskell.packages.${compiler}.developPackage {
  name = builtins.baseNameOf ./.;
  root = gitignoreSource ./.;
  overrides = self: super: with pkgs.haskell.lib;

  # Working on getting this function upstreamed into nixpkgs.
  # (See https://github.com/NixOS/nixpkgs/pull/52848 for status)
  # This actually gets things directly from hackage and doesn't
  # depend on the state of nixpkgs.  Allows you to have fewer
  # fetchFromGitHub overrides.
  let callHackageDirect = {pkg, ver, sha256}@args:
        let pkgver = "${pkg}-${ver}";
        in self.callCabal2nix pkg (pkgs.fetchzip {
             url = "http://hackage.haskell.org/package/${pkgver}/${pkgver}.tar.gz";
             inherit sha256;
           }) {};
  in {
    # Don't run a package's test suite
    # foo = dontCheck super.foo;
    #
    # Don't enforce package's version constraints
    # bar = doJailbreak super.bar;
    #
    # Get a specific hackage version straight from hackage. Unlike the above
    # callHackage approach, this will always succeed if the version is on
    # hackage. The downside is that you have to specify the hash manually.

    beam-postgres = dontCheck super.beam-postgres;

    blake2 = callHackageDirect {
      pkg = "blake2";
      ver = "0.3.0";
      sha256 = "0n366qqhz7azh9fgjqvj99b3ni57721a2q5xxlawwmkxrxy36hb2";
    };

    scheduler = callHackageDirect {
      pkg = "scheduler";
      ver = "1.4.2.2";
      sha256 = "16ljs9sypcc90rnnc4kdqi2y7dm6nri2nvisdy6n17gl7vmvy4vq";
    };

    # To discover more functions that can be used to modify haskell
    # packages, run "nix-repl", type "pkgs.haskell.lib.", then hit
    # <TAB> to get a tab-completed list of functions.
  };

  source-overrides = {
    beam-core = "${beam-src}/beam-core";
    beam-migrate = "${beam-src}/beam-migrate";
    beam-postgres = "${beam-src}/beam-postgres";

    chainweb-api = pkgs.fetchFromGitHub {
      owner = "kadena-io";
      repo = "chainweb-api";
      rev = "0792df840e49821f60a8e6e5a1cdc6c5862137a1";
      sha256 = "0r1b0fch6dcxnhlpdmdbqgdy7y6gi71ggvwk9l7fn9v77vd86q05";
    };

    streaming-events = pkgs.fetchFromGitHub {
      owner = "kadena-io";
      repo = "streaming-events";
      rev = "f949c4c20b848d1e6197dc41c2ead14a64e12652";
      sha256 = "1riqi1r1gaa5a3av9a25mc4zvaqzaqzcycccvd7mrybkxs3zcjwj";
    };
  };
  modifier = drv: pkgs.haskell.lib.overrideCabal drv (attrs: {
    buildTools = (attrs.buildTools or []) ++ [
      pkgs.haskell.packages.${compiler}.cabal-install
      pkgs.haskell.packages.${compiler}.ghcid
    ];
  });
}
