-- Offshore Instruments Database
EXEC sys.sp_cdc_enable_db;
GO

-- Create table
create table dbo.datapoints
(
    id        bigint identity
        primary key,
    asset_id  int            not null,
    timestamp datetime2      not null,
    value     decimal(18, 5) not null
);
go

-- Then enable CDC on the table
EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo',          -- The schema where the table resides
     @source_name   = N'datapoints',  -- The name of the table to enable CDC for
     @role_name     = NULL,            -- NULL means no gating role is required to view change data
     @supports_net_changes = 1;       -- Set to 1 because 'work_permit' has a PRIMARY KEY (id)
GO

INSERT INTO datapoints (asset_id, timestamp, value)
VALUES (8, CURRENT_TIMESTAMP,20.0);

SELECT * FROM datapoints;

-- Work Permit Database
EXEC sys.sp_cdc_enable_db;
GO

-- Create the tables
create table dbo.asset
(
    id           int identity
        primary key,
    tag          nvarchar(100)              not null
        unique,
    name         nvarchar(255)              not null,
    description  nvarchar(max),
    status       nvarchar(50)               not null,
    date_created datetime default getdate() not null,
    last_updated datetime
);

create table dbo.work_permit
(
    id                 int identity
        primary key,
    description        nvarchar(max)          not null,
    status             nvarchar(50)           not null,
    type               nvarchar(50)           not null,
    responsible_person nvarchar(255)          not null,
    permit_number      as 'WP-' + right('00000' + CONVERT([varchar](5), [id]), 5),
    issue_date         date default getdate() not null,
    valid_from         datetime               not null,
    valid_to           datetime               not null,
    authorized_by      nvarchar(255),
    location           nvarchar(255),
    notes              nvarchar(max)
);

create table dbo.permit_asset
(
    permit_id int not null
        references dbo.work_permit,
    asset_id  int not null
        references dbo.asset,
    primary key (permit_id, asset_id)
);
go

EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',          -- The schema where the table resides
@source_name   = N'work_permit',  -- The name of the table to enable CDC for
@role_name     = NULL,            -- NULL means no gating role is required to view change data
@supports_net_changes = 1;        -- Set to 1 because 'work_permit' has a PRIMARY KEY (id)
EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo',          -- The schema where the table resides
     @source_name   = N'permit_asset',  -- The name of the table to enable CDC for
     @role_name     = NULL,            -- NULL means no gating role is required to view change data
     @supports_net_changes = 1;        -- Set to 1 because 'work_permit' has a PRIMARY KEY (id)EXEC sys.sp_cdc_enable_table
EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo',          -- The schema where the table resides
     @source_name   = N'asset',  -- The name of the table to enable CDC for
     @role_name     = NULL,            -- NULL means no gating role is required to view change data
     @supports_net_changes = 1;        -- Set to 1 because 'work_permit' has a PRIMARY KEY (id)
GO

-- Asset
INSERT INTO asset(tag, name, description, status, last_updated)
VALUES (N'HELLO-P2020-T', N'SuperPump',
        N'Pumps every day except sunday.', N'IN_USE', GETDATE());
SELECT TOP 1 * FROM asset ORDER BY id DESC;

-- Work Permit
INSERT INTO work_permit(description, status, type, responsible_person, valid_from, valid_to, authorized_by, location)
VALUES (N'Very important work permit',
        N'IN_PROGRESS', N'TEMP_MAINTENANCE', N'Olav',
        '2025-10-22', '2025-10-23', N'Jonas G. Støre', N'Tau Scene');
SELECT TOP 1 * FROM work_permit ORDER BY id DESC;

-- Work Permit Asset Relation
INSERT INTO permit_asset(permit_id, asset_id) VALUES (9, 8);
SELECT TOP 1 * FROM permit_asset ORDER BY permit_id DESC;