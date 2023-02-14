# WE PREFER YOU USE "nix build .#"chainweb-data-docker" TO BUILD THE DOCKER IMAGE
# INSTEAD OF "nix-build docker-legacy.nix" (BOTH WORK)
{ chainweb-data ? import ./.
, dockerTag ? "latest"
, baseImageDef ? {
      imageName = "ubuntu";
      imageDigest = "sha256:965fbcae990b0467ed5657caceaec165018ef44a4d2d46c7cdea80a9dff0d1ea";
      sha256 = "10wlr8rhiwxmz1hk95s9vhkrrjkzyvrv6nshg23j86rw08ckrqnz";
      finalImageTag = "22.04";
      finalImageName = "ubuntu";
      }
, pkgs ?
    let flakeFile = with builtins; fromJSON (readFile ./flake.lock);
        rev = flakeFile.nodes.nixpkgs-unstable.locked.rev;
        sha256 = flakeFile.nodes.nixpkgs-unstable.locked.narHash;
    in import (builtins.fetchTarball {
         url = "https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz";
         inherit sha256;
         }) {}
, ... }:

let baseImage = pkgs.dockerTools.pullImage baseImageDef;
in pkgs.dockerTools.buildImage {
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
