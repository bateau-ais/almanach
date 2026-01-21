{ pkgs, lib, config, inputs, ... }:

{
  # https://devenv.sh/basics/
  env.GREET = "devenv";

  # https://devenv.sh/packages/
  packages = [ pkgs.git ];

  languages.python = {
    enable = true;
    package = pkgs.python314;
    uv = {
      enable = true;
      sync = {
        enable = true;
      };
    };
    venv.enable = true;
  };
}
