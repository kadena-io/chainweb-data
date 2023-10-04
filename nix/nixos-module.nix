{ pkgs
, lib
, config
, ...
}:
with lib;
let
  cfg = config.services.chainweb-data;
  pgPort = config.services.postgresql.port;
  dbParams =
    if cfg.dbstring != null then
      "--dbstring ${cfg.dbstring}"
    else if cfg.dbname != null then
      let
        passArg = optionalString (cfg.dbpassFile != null)
          "--dbpass \"$(cat ${cfg.dbpassFile})\"";
      in
      "--dbname ${cfg.dbname} --dbuser ${cfg.dbuser} --dbhost ${cfg.dbhost} " +
      "--dbport ${toString cfg.dbport} ${passArg}"
    else if cfg.dbdir != null then
      "--dbdir ${cfg.dbdir}"
    else abort "No chainweb-data database configuration provided"
    ;
  cwnParams = "--service-host ${cfg.serviceHost} --service-port ${toString cfg.servicePort} " +
    optionalString cfg.serviceHttps "--service-https";
  logParams = "--level ${cfg.level}";
  migrationParams = optionalString cfg.migrate "--migrate " +
    optionalString cfg.ignoreSchemaDiff "--ignore-schema-diff " +
    optionalString (cfg.migrationsFolder != null) "--migrations-folder ${cfg.migrationsFolder} " +
    optionalString (cfg.extraMigrationsFolder != null) "--extra-migrations-folder ${cfg.extraMigrationsFolder}";
  externalDb = true;
  cwd-with-conn-params = pkgs.writeShellScript "cwd-with-conn-params" ''
    ${cfg.package}/bin/chainweb-data ${dbParams} ${cwnParams} "$@"
  '';
  chainweb-data-fill = pkgs.writeShellScript "chainweb-data-fill" ''
    ${cwd-with-conn-params} fill
  '';
in
{
  options.services.chainweb-data = {
    enable = mkEnableOption "chainweb-data";
    package = mkOption {
      type = types.package;
      description = "The chainweb-data package to use";
      default = pkgs.chainweb-data;
    };
    database = mkOption {
      type = types.str;
      description = "The database name";
      default = "chainweb-data";
    };
    port = mkOption {
      type = types.int;
      description = "The chainweb-data HTTP API port";
      default = 1849;
    };
    dbstring = mkOption {
      type = types.nullOr types.str;
      description = "Postgres Connection String";
      default = null;
    };
    dbhost = mkOption {
      type = types.str;
      description = "Postgres DB hostname";
      default = "localhost";
    };
    dbport = mkOption {
      type = types.int;
      description = "Postgres DB port";
      default = 5432;
    };
    dbuser = mkOption {
      type = types.str;
      description = "Postgres DB user";
      default = "postgres";
    };
    dbpassFile = mkOption {
      type = types.nullOr types.str;
      description = "Postgres DB password file";
      default = null;
    };
    dbname = mkOption {
      type = types.nullOr types.str;
      description = "Postgres DB name";
      default = null;
    };
    dbdir = mkOption {
      type = types.nullOr types.str;
      description = "Directory for self-run postgres";
      default = null;
    };
    serviceHttps = mkOption {
      type = types.bool;
      description = "Use HTTPS to connect to the service API (instead of HTTP)";
      default = false;
    };
    serviceHost = mkOption {
      type = types.str;
      description = "host for the service API";
      default = "localhost";
    };
    servicePort = mkOption {
      type = types.int;
      description = "port for the service API (default 1848)";
      default = 1848;
    };
    level = mkOption {
      type = types.str;
      description = "Initial log threshold";
      default = "info";
    };
    migrate = mkOption {
      type = types.bool;
      description = "Run DB migration";
      default = true;
    };
    ignoreSchemaDiff = mkOption {
      type = types.bool;
      description = "Ignore any unexpected differences in the database schema";
      default = false;
    };
    migrationsFolder = mkOption {
      type = types.nullOr types.str;
      description = "Path to the migrations folder";
      default = null;
    };
    extraMigrationsFolder = mkOption {
      type = types.nullOr types.str;
      description = "Path to extra migrations folder";
      default = null;
    };
  };

  # Implementation
  config = mkIf cfg.enable {
    users.users.chainweb-data = {
      description = "Chainweb Data";
      isSystemUser = true;
      group = "chainweb-data";
    };
    users.groups.chainweb-data = {};

    systemd.services.chainweb-data = {
      description = "Chainweb Data";
      after = [ "network.target" ];
      wantedBy = [ "multi-user.target" ];
      serviceConfig = {
        Type = "simple";
        ExecStart = ''
          ${cwd-with-conn-params} \
            ${logParams} ${migrationParams} \
            server --port ${toString cfg.port} --serve-swagger-ui
        '';
        Restart = "always";
        RestartSec = 5;
        User = "chainweb-data";
      };
    };

    systemd.services.chainweb-data-fill = {
      description = "Periodic Chainweb Data Fill";
      after = [ "network.target" "chainweb-data.service" ];
      wants = [ "chainweb-data.service" ];
      wantedBy = [ "multi-user.target" ];
      serviceConfig = {
        Type = "oneshot";
        ExecStart = ''
          ${chainweb-data-fill}
        '';
        User = "chainweb-data";
      };
    };
    systemd.timers.chainweb-data-fill = {
      description = "Periodic Chainweb Data Fill";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnCalendar = "hourly";
        Persistent = true;
      };
    };
  };
}