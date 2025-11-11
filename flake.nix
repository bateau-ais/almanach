{
  description = "almanach development and build flake";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    uv2nix.url = "github:pyproject-nix/uv2nix";
    pyproject-nix.url = "github:pyproject-nix/pyproject.nix";
    pyproject-build-systems.url = "github:pyproject-nix/build-system-pkgs";

    uv2nix.inputs.nixpkgs.follows = "nixpkgs";
    pyproject-nix.inputs.nixpkgs.follows = "nixpkgs";
    pyproject-build-systems.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = {
    self,
    nixpkgs,
    uv2nix,
    pyproject-nix,
    pyproject-build-systems,
  }: let
    systems = [
      "x86_64-linux"
      "aarch64-linux"
    ];
    forAllSystems = nixpkgs.lib.genAttrs systems;
    workspace = uv2nix.lib.workspace.loadWorkspace {workspaceRoot = ./.;};
    mkContext = system: let
      pkgs = import nixpkgs {inherit system;};
      python = pkgs.python314;
      pythonBase = pkgs.callPackage pyproject-nix.build.packages {inherit python;};
      overlay = workspace.mkPyprojectOverlay {sourcePreference = "wheel";};
      pythonSet = pythonBase.overrideScope (
        pkgs.lib.composeManyExtensions [
          pyproject-build-systems.overlays.wheel
          overlay
        ]
      );
      editablePythonSet = pythonSet.overrideScope (
        workspace.mkEditablePyprojectOverlay {root = "$REPO_ROOT";}
      );
      devEnv = editablePythonSet.mkVirtualEnv "almanach-dev" workspace.deps.all;
    in {
      inherit pkgs pythonSet editablePythonSet devEnv;
    };
    contexts = forAllSystems mkContext;
  in {
    packages = forAllSystems (system: let
      ctx = contexts.${system};
    in {
      almanach = ctx.pythonSet.almanach;
      default = ctx.pythonSet.almanach;
    });

    devShells = forAllSystems (system: let
      ctx = contexts.${system};
    in {
      default = ctx.pkgs.mkShell {
        packages = [
          ctx.devEnv
          ctx.pkgs.uv
        ];
        env = {
          UV_NO_SYNC = "1";
          UV_PYTHON = ctx.editablePythonSet.python.interpreter;
          UV_PYTHON_DOWNLOADS = "never";
        };
        shellHook = ''
          unset PYTHONPATH
          export REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
        '';
      };
    });
  };
}
