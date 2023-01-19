{ compiler ? "ghc8107"
, rev      ? "7a94fcdda304d143f9a40006c033d7e190311b54"
, sha256   ? "0d643wp3l77hv2pmg2fi7vyxn4rwy0iyr8djcw1h5x72315ck9ik"
, pkgs     ?
    import (builtins.fetchTarball {
      url    = "https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz";
      inherit sha256; }) {
      config.allowBroken = false;
      config.allowUnfree = true;
    }
, returnShellEnv ? false
, mkDerivation ? null
}:
let gitignoreSrc = import (pkgs.fetchFromGitHub {
      owner = "hercules-ci";
      repo = "gitignore";
      rev = "9e80c4d83026fa6548bc53b1a6fab8549a6991f6";
      sha256 = "04n9chlpbifgc5pa3zx6ff3rji9am6msrbn1z3x1iinjz2xjfp4p";
    }) {};
    nix-thunk = import ./deps/nix-thunk {};
    chainwebDataSrc = pkgs.lib.cleanSourceWith {
      filter = path: type:
        # Filter out these files so that nix doesn't recompile CW-D every time
        # these files change, they're not relevant to the Haskell build anyway.
        let ignored = map toString [./docker.nix ./default.nix];
        in !(builtins.elem path ignored);
      src = gitignoreSrc.gitignoreSource ./.;
      name = "chainweb-data-src";
    };

in
pkgs.haskell.packages.${compiler}.developPackage {
  name = "chainweb-data";
  root = chainwebDataSrc;

  overrides = self: super: with pkgs.haskell.lib;
    let gargoylePkgs = import ./deps/gargoyle { haskellPackages = self; };
    in {
      inherit (gargoylePkgs) gargoyle gargoyle-postgresql;

      statistics = self.callHackageDirect {
        pkg = "statistics";
        ver = "0.15.2.0";
        sha256 = "1sg1gv2sc8rdsl6qby6p80xv3iasy6w2khbkc6cx7j2iva67v33r";
      } {};

      direct-sqlite = self.callHackageDirect {
        pkg = "direct-sqlite";
        ver = "2.3.27";
        sha256 = "0w8wj3210h08qlws40qhidkscgsil3635zk83kdlj929rbd8khip";
       } {};

      pretty-simple = dontCheck (self.callHackageDirect {
        pkg = "pretty-simple";
        ver = "3.3.0.0";
        sha256 = "19zwzzvjgadmzp9gw235bsr6wkljr8k0cqy75h5q8n0d5m60ip7m";
      } {});

      resource-pool = self.callHackageDirect {
        pkg = "resource-pool";
        ver = "0.3.0.0";
        sha256 = "0bpf868b6kq1g83s3sad26kfsawmpd3j0xpkyab8370lsq6zhcs1";
      } {};

      pact = appendConfigureFlag super.pact "-f-build-tool";

      chainweb-api     = doJailbreak super.chainweb-api;
      chainweb-storage = dontCheck super.chainweb-storage;

      beam-automigrate = doJailbreak super.beam-automigrate;
      beam-core        = doJailbreak super.beam-core;
      beam-migrate     = doJailbreak super.beam-migrate;
      beam-postgres    = doJailbreak super.beam-postgres;
      hashable         = doJailbreak super.hashable;
      rebase           = doJailbreak super.rebase;
      streaming-events = unmarkBroken (doJailbreak super.streaming-events);
      tmp-postgres     = dontCheck (doJailbreak super.tmp-postgres);
      token-bucket     = unmarkBroken super.token-bucket;

      # Cuckoo tests fail due to a missing symbol
      cuckoo        = dontCheck super.cuckoo;

      # These tests pull in unnecessary dependencies
      http2         = dontCheck super.http2;
      prettyprinter = dontCheck super.prettyprinter;
      aeson         = dontCheck super.aeson;
      generic-data  = dontCheck super.generic-data;
  };

  source-overrides = {
    beam-automigrate = nix-thunk.thunkSource ./deps/beam-automigrate;
    chainweb-api     = nix-thunk.thunkSource ./deps/chainweb-api;
    pact             = nix-thunk.thunkSource ./deps/pact;

    OneTuple                    = "0.3";
    aeson                       = "1.5.6.0";
    ansi-terminal               = "0.11.3";
    prettyprinter-ansi-terminal = "1.1.2";
    time-compat                 = "1.9.5";
    trifecta                    = "2.1.1";
    unordered-containers        = "0.2.15.0";

    http-client     = "0.6.4.1";
    http-client-tls = "0.3.5.3";
    retry           = "0.8.1.2";

    # These are required in order to not break payload validation
    base16-bytestring = "0.1.1.7";
    prettyprinter     = "1.6.0";
    hashable          = "1.3.0.0";
    base64-bytestring = "1.0.0.3";
  };

  modifier = drv: pkgs.haskell.lib.overrideCabal drv (attrs: {
    buildTools = (attrs.buildTools or []) ++ [
      pkgs.zlib
      pkgs.haskell.packages.${compiler}.cabal-install
    ];
  });

  inherit returnShellEnv;
}
