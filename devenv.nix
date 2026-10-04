{ pkgs, lib, config, inputs, ... }:

{
  # https://devenv.sh/basics/
  env.GREET = "devenv";

  # Build dependencies from flake.nix's default devShell.
  packages = with pkgs; [
    git
    cmake
    pkg-config
    clang
    llvmPackages.libclang
    alsa-lib
    openssl
    rust-analyzer
    rustfmt
    clippy
    wtype
    dotool
    wl-clipboard
    ydotool
    xdotool
    xclip
    libnotify
    pciutils
  ];

  # https://devenv.sh/languages/
  languages.rust.enable = true;

  env.LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";

  # https://devenv.sh/processes/
  # processes.dev.exec = "${lib.getExe pkgs.watchexec} -n -- ls -la";

  # https://devenv.sh/services/
  # services.postgres.enable = true;

  # https://devenv.sh/scripts/
  scripts.hello.exec = ''
    echo hello from $GREET
  '';

  # https://devenv.sh/basics/
  enterShell = ''
    hello         # Run scripts directly
    git --version # Use packages
  '';

  # https://devenv.sh/tasks/
  # tasks = {
  #   "myproj:setup".exec = "mytool build";
  #   "devenv:enterShell".after = [ "myproj:setup" ];
  # };

  # https://devenv.sh/tests/
  enterTest = ''
    echo "Running tests"
    git --version | grep --color=auto "${pkgs.git.version}"
  '';

  # https://devenv.sh/git-hooks/
  # git-hooks.hooks.shellcheck.enable = true;

  # See full reference at https://devenv.sh/reference/options/
}
