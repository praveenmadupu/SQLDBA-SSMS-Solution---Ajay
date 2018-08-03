exec sp_configure 'show advanced options', 1 ;  
GO  
RECONFIGURE ;  
GO  
exec sp_configure 'blocked process threshold', 20 ; -- 1 minutes  
GO  
RECONFIGURE ;  
GO