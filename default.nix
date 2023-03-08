let inputs = (import (
      fetchTarball {
         url = "https://github.com/edolstra/flake-compat/archive/35bb57c0c8d8b62bbfd284272c928ceb64ddbde9.tar.gz";
         sha256 = "1prd9b1xx8c0sfwnyzkspplh30m613j42l1k789s521f4kv4c2z2"; }
     ) {
       src =  ./.;
     }).defaultNix.inputs;
    pkgsDef = import inputs.nixpkgs (import inputs.haskellNix {}).nixpkgsArgs;
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
    chainweb-data = pkgs.haskell-nix.project' {
	    src = ./.;
      index-state = "2023-02-01T00:00:00Z";
	    compiler-nix-name = "ghc8107";
      projectFileName = "cabal.project";
	    shell.tools = {
	      cabal = {};
	    };
      modules = if enableProfiling then [ profilingModule ] else [];
    };
    flake = chainweb-data.flake {};
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

       runAsRoot = ''
         ln -s "${default}/bin/chainweb-data" /usr/local/bin/
         mkdir -p /chainweb-data
         '';

       config = {
         WorkingDir = "/chainweb-data";
         Volumes = { "/chainweb-data" = {}; };
         Entrypoint = [ "chainweb-data" ];
       };
    };
  signing-api-src = builtins.fetchTarball {
    url = https://github.com/kadena-io/signing-api/archive/9124a37d4847f3959d3a4ef21405e93b97525632.tar.gz;
    sha256 = "sha256:1ml0i155xywk3gbbp8shxjfbzxkmh98wby1zfds4sys5x8iz35kz";
  };
  signing-api = import signing-api-src {};
  all = with signing-api; pkgs.writeScript "all" ''
    ${schemathesis}
    ${api.swagger-cli}
    ${signingProject.shells.ghc}
  '';
in {
  inherit all;
}
