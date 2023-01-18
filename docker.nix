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
  sha256 = "04amkx2mgwzzf6r14fgn4x38s69s0rxmm8ik2cnh911cdbqvwbn1";
  finalImageTag = "22.04";
  finalImageName = "ubuntu";
};
in pkgs.dockerTools.buildImage {
  name = "chainweb-data-docker";
  tag = dockerTag;
  contents = [pkgs.postgresql_12 pkgs.curl pkgs.sqlite pkgs.bash pkgs.coreutils];

  fromImage = ubuntuFromDockerHub;

  runAsRoot = ''
    #!${pkgs.runtimeShell}
    ${pkgs.dockerTools.shadowSetup}
    mkdir -p /chainweb-data
    '';

  config = {
    Cmd = [ "--help"];
    WorkingDir = "/chainweb-data";
    Volumes = { "/chainweb-data" = {}; };
    Entrypoint = [ "${pkgs.haskell.lib.justStaticExecutables chainweb-data}/bin/chainweb-data" ];
  };

}
