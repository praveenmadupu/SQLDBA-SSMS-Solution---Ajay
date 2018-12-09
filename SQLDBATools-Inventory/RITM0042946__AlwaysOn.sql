/*
Ajay,
  We have two availability group setups in our environment. The first one is on TUL1SKYPEBEDB1 and DB2 and the second one is DAL2SKYPEBEDB1 and DB2. We need to configure monitoring on these servers where DBAs are notified if any of the replicas ever goes into a RESOLVING state. Ideally there should be a job that should check for a status every 5 minutes and send an email out to the DBAs if such an event occurs.
*/

select * from SQLDBATools.info.Instance as i
	where i.IsHadrEnabled = 1