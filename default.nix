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
    # aeson = callHackageDirect {
    #   pkg = "aeson";
    #   ver = "1.4.2.0";
    #   sha256 = "0qcczw3l596knj9s4ha07wjspd9wkva0jv4734sv3z3vdad5piqh";
    # };
    #
    # To discover more functions that can be used to modify haskell
    # packages, run "nix-repl", type "pkgs.haskell.lib.", then hit
    # <TAB> to get a tab-completed list of functions.
  };
  source-overrides = {
    # Use a specific hackage version using callHackage. Only works if the
    # version you want is in the version of all-cabal-hashes that you have.
    # bytestring = "0.10.8.1";
    #
    # Use a particular commit from github
    chainweb-api = pkgs.fetchFromGitHub {
      owner = "kadena-io";
      repo = "chainweb-api";
      rev = "07a515e9b5401b73ff59af55b369590273decce7";
      sha256 = "1jak9b7adydhryfv6znywv7djxddipy1p22g33y88jkc7zrbr568";
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
