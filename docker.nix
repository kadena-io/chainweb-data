{ kpkgs ? import ./deps/kpkgs {}
, chainweb-data ? import ./. {}
, dockerTag ? "latest"
, rev ? "a7ecde854aee5c4c7cd6177f54a99d2c1ff28a31"
, sha256 ? "162dywda2dvfj1248afxc45kcrg83appjd0nmdb541hl7rnncf02"
, pkgs ?
  import (builtins.fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz";
    inherit sha256;
  }) {
    config.allowBroken = false;
    config.allowUnfree = true;
  }
, ... }:

let ubuntuFromDockerHub = pkgs.dockerTools.pullImage {
  imageName = "ubuntu";
  imageDigest = "sha256:965fbcae990b0467ed5657caceaec165018ef44a4d2d46c7cdea80a9dff0d1ea";
  sha256 = "10wlr8rhiwxmz1hk95s9vhkrrjkzyvrv6nshg23j86rw08ckrqnz";
  finalImageTag = "22.04";
  finalImageName = "ubuntu";
};
in pkgs.dockerTools.buildImage {
  name = "chainweb-data";
  tag = dockerTag;

  fromImage = ubuntuFromDockerHub;

  runAsRoot = ''
    ln -s "${pkgs.haskell.lib.justStaticExecutables chainweb-data}/bin/chainweb-data" /usr/local/bin/
    mkdir -p /chainweb-data
    '';

  config = {
    WorkingDir = "/chainweb-data";
    Volumes = { "/chainweb-data" = {}; };
    Entrypoint = [ "chainweb-data" ];
  };

}
