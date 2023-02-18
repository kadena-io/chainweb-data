# WE PREFER YOU USE "nix build .#"chainweb-data-docker" TO BUILD THE DOCKER IMAGE
# INSTEAD OF "nix-build docker-legacy.nix" (BOTH WORK)
let inputs = import ./flake-inputs.nix; in
{ chainweb-data ? (import ./. {}).default
, dockerTag ? "latest"
, baseImageDef ? {
      imageName = "ubuntu";
      imageDigest = "sha256:965fbcae990b0467ed5657caceaec165018ef44a4d2d46c7cdea80a9dff0d1ea";
      sha256 = "10wlr8rhiwxmz1hk95s9vhkrrjkzyvrv6nshg23j86rw08ckrqnz";
      finalImageTag = "22.04";
      finalImageName = "ubuntu";
      }
, nixpkgs ? import inputs.nixpkgs.sourceInfo.outPath {}
, ... }:

let baseImage = nixpkgs.dockerTools.pullImage baseImageDef;
in nixpkgs.dockerTools.buildImage {
  name = "chainweb-data";
  tag = dockerTag;

  fromImage = baseImage;

  runAsRoot = ''
    ln -s "${chainweb-data}/bin/chainweb-data" /usr/local/bin/
    mkdir -p /chainweb-data
    '';

  config = {
    WorkingDir = "/chainweb-data";
    Volumes = { "/chainweb-data" = {}; };
    Entrypoint = [ "chainweb-data" ];
  };

}
