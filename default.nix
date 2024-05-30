let inputs = (import (
      fetchTarball {
         url = "https://github.com/edolstra/flake-compat/archive/35bb57c0c8d8b62bbfd284272c928ceb64ddbde9.tar.gz";
         sha256 = "1prd9b1xx8c0sfwnyzkspplh30m613j42l1k789s521f4kv4c2z2"; }
     ) {
       src =  ./.;
     }).defaultNix.inputs;
    hs-nix-infra = inputs.hs-nix-infra;
    pkgsDef = import hs-nix-infra.nixpkgs {
      config = hs-nix-infra.haskellNix.config;
      overlays = [ hs-nix-infra.haskellNix.overlay] ;
    };
in
{ pkgs ? pkgsDef
, dontStrip ? false
, threaded ? true
, enableProfiling ? false
, dockerTag ? "latest"
, baseImageDef ? {
      imageName = "ubuntu";
      imageDigest = "sha256:965fbcae990b0467ed5657caceaec165018ef44a4d2d46c7cdea80a9dff0d1ea";
      sha256 = "10wlr8rhiwxmz1hk95s9vhkrrjkzyvrv6nshg23j86rw08ckrqnz";
      finalImageTag = "22.04";
      finalImageName = "ubuntu";
      }
, ...
}:
let profilingModule = {
      enableLibraryProfiling = enableProfiling;
      enableProfiling = enableProfiling;
    };
    project = pkgs.haskell-nix.cabalProject' {
	    src = ./.;
	    compiler-nix-name = "ghc963";
	    shell.tools = {
	      cabal = {};
	    };
      modules =
        pkgs.lib.optional enableProfiling profilingModule ++
        [{
          packages.pact.components.library.ghcOptions = [ "-Wwarn" ];
        }];

    };
    flake = project.flake';
    default = flake.packages."chainweb-data:exe:chainweb-data".override (old: {
      inherit dontStrip;
      flags = old.flags // {
        inherit threaded;
      };
    });
    baseImage = pkgs.dockerTools.pullImage baseImageDef;
    dockerImage = pkgs.dockerTools.buildImage {
       name = "chainweb-data";
       tag = dockerTag;

       fromImage = baseImage;

       copyToRoot = pkgs.runCommand "copyToRoot" {} ''
         mkdir -p $out/usr/local/bin/
         ln -s "${default}/bin/chainweb-data" $out/usr/local/bin/
         mkdir -p $out/chainweb-data
        '';

       config = {
         WorkingDir = "/chainweb-data";
         Volumes = { "/chainweb-data" = {}; };
         Entrypoint = [ "chainweb-data" ];
       };
    };
in {
  inherit project flake default dockerImage;
}
