let inputs = import ./flake-inputs.nix;
    pkgsDef = import inputs.nixpkgs (import inputs.haskellNix {}).nixpkgsArgs;
in
{ pkgs ? pkgsDef
, dontStrip ? false
, threaded ? true
, enableProfiling ? false
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
in {
  inherit flake default;
}