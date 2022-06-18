{ kpkgs ? import ./deps/kpkgs {}
, chainweb-data ? import ./. {}
, dockerTag ? "latest"
, rev ? "7e9b0dff974c89e070da1ad85713ff3c20b0ca97"
, sha256 ? "1ckzhh24mgz6jd1xhfgx0i9mijk6xjqxwsshnvq789xsavrmsc36"
, pkgs ?
  import (builtins.fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz";
    inherit sha256;
  }) {
    config.allowBroken = false;
    config.allowUnfree = true;
  }
, ... }:

kpkgs.pkgs.dockerTools.buildImage {
  name = "chainweb-data-docker";
  tag = dockerTag;
  contents = [pkgs.postgresql_12 pkgs.curl pkgs.sqlite];

  runAsRoot = ''
    #!${kpkgs.pkgs.runtimeShell}
    ${kpkgs.pkgs.dockerTools.shadowSetup}
    mkdir -p /chainweb-data
    '';

  config = {
    Cmd = [ "--help"];
    WorkingDir = "/chainweb-data";
    Volumes = { "/chainweb-data" = {}; };
    Entrypoint = [ "${kpkgs.pkgs.haskell.lib.justStaticExecutables chainweb-data}/bin/chainweb-data" ];
  };

}
