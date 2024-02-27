--------------------------------------------------------------------------------------------------------------------------------------
--  sysadmin fixed server role or the db_owner fixed database role can execute the script
--------------------------------------------------------------------------------------------------------------------------------------
--  Version history 

--  2024-02-14 - Initial revision 
--  2024-02-15 -- Added instructions and creation as a Store Procedure and Job

-- Usage instructions: 

-- Step 1 : run this SQL to create Stored Procedure in the master DB

-- Step 2 : Create a SQL Agent Job to Execute this procedure on a 30 minute schedule
-- Example Step in the SQL Agent Job:
-- (as master DB) 
-- EXEC [dbo].[MSJETLogRetentionAOG]
-- @dbName = N'mstest',
-- @retentionminutes = 4320

-- Step 3 : add additional Job steps change the above step 2 to additional databases. 
-- Set each prior step to continue to to the next step on success and the final step to fail on error

-- Step 4 : set Job Schedule to run every day on 30 minute interval with out ending 
-- Step 5 : enable the Job and start it

USE [master] -- change this line to DB you have Stored Procedure CREATE and EXEC Permissions
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[MSJETLogRetentionAOG]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[MSJETLogRetentionAOG] AS'
END
GO
ALTER PROCEDURE [dbo].[MSJETLogRetentionAOG]

--------------------------------------------------------------------------------------------------------------------------------------
-- Database name
@dbName nvarchar(255) = N'master',
-- 3 days in minute granularity 
@retentionminutes int = 4320 --(3 * 24 * 60) = 72 hours
--------------------------------------------------------------------------------------------------------------------------------------

AS

BEGIN
	declare @isHealthy int
	declare @formatTS varchar(128) = 'yyyy/MM/dd HH:mm:ss.fff' -- DO NOT CHANGE
	select @isHealthy = count(*) from sys.availability_replicas ar
	JOIN sys.dm_hadr_availability_replica_states ars 
		ON ar.replica_id = ars.replica_id
	JOIN sys.dm_hadr_database_replica_states rs 
		ON ar.replica_id = rs.replica_id
	where ars.group_id = ar.group_id and (rs.synchronization_health_desc != 'HEALTHY' or rs.synchronization_state_desc != 'SYNCHRONIZED')
	if (@isHealthy = 0)
	begin
		if sys.fn_hadr_is_primary_replica (@dbName) = 1   
		begin  
			declare @redone_last_time datetime
			declare @trunctime datetime
			
			select @redone_last_time = min(dhdrs.last_redone_time)
			from sys.dm_hadr_database_replica_states dhdrs
				inner join sys.dm_hadr_database_replica_states dhdr
					on dhdrs.group_id = dhdr.group_id
			where dhdrs.database_id = DB_ID(@dbName) and dhdrs.last_redone_time is not null

			-- Set trunction time
			set @trunctime = dateadd(minute, -@retentionminutes, getdate())

			print 'The redone last time : ' + CONVERT(varchar(20), @redone_last_time) 
			print 'Truncation time : ' + CONVERT(varchar(20), @trunctime)   

			-- Get the recvery checkpoint
			declare @recovery_cp binary(10)
			declare @end_time datetime
			select top (1)
				@recovery_cp
					= convert(binary(10), replace(coalesce(nullif([Oldest Active LSN], 'none'), [Previous Lsn]), ':', ''), 2),
				@end_time 
					= [End Time]
			from fn_dblog(default, default)
			where [Operation] = 'LOP_COMMIT_XACT'
				  and [End Time] <= FORMAT(@trunctime, @formatTS)
			order by [End Time]  desc

			print  'recovery checkpoint is :' + master.sys.fn_varbintohexstr(@recovery_cp)

			if (@end_time > @redone_last_time)
			begin
				print 'Commit timestamp is greater than last redone time. Find recovery checkpoint again.'
				select top (1)
					@recovery_cp
						= convert(binary(10), replace(coalesce(nullif([Oldest Active LSN], 'none'), [Previous Lsn]), ':', ''), 2),
					@end_time 
						= [End Time]
				from fn_dblog(default, default)
				where [Operation] = 'LOP_COMMIT_XACT'
					  and [End Time] <= FORMAT(@redone_last_time, @formatTS)
				order by [End Time] desc
				print  'new recovery checkpoint is :' + master.sys.fn_varbintohexstr(@recovery_cp)
			end

			set nocount on
			declare @begtran binary(10)
			declare @endtran binary(10)

			declare @trans table (begt binary(10), endt binary(10))
			insert into @trans exec sp_repltrans

			select top (2)
				@begtran = begt,
				@endtran = endt
			from @trans
			where begt <= @recovery_cp
			order by begt desc
			set nocount off

			print 'begin [' + isnull(master.sys.fn_varbintohexstr(@begtran), 'null')  + '] , End [' + isnull(master.sys.fn_varbintohexstr(@endtran), 'null') + ']'
		
			-- Check if we get rows back 
			if @begtran is not null and @endtran is not null
			begin 
				exec sp_repldone @xactid = @begtran, @xact_seqno = @endtran, @numtrans = 0, @time = 0, @reset = 0 
			end
			else 
			begin
				print 'No rows are available'
			end
		end
		else 
		begin
			print 'The connection is not made with the primary replica.'
		end
	end
	else 
	begin
		print 'All replicas are not in a healthy state. To execute this script, all replicas must be in a healthy state.'
	end

END
