#if you like to force TLS 1.2 for your connection to Azure
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
##=============
# Functions
##=============
function GetSourceData {
    Param(  
        [string]$SQLServer,
        [string]$DBName,
		[string]$TableName,
        [string]$SqlLogin,
        [string]$Password
    )  

$Src_SqlQuery = "select * from $TableName"

$Src_SqlConnection = New-Object System.Data.SqlClient.SqlConnection

##Azure Connection string
$Src_SqlConnection.ConnectionString = "Data Source=tcp:$SQLServer;Initial Catalog=$DBName;User Id=$SqlLogin;Password=$Password;Current Language=us_english;Encrypt=True;trustServerCertificate=false"

write-host "Data Source=tcp:$SQLServer;Initial Catalog=$DBName;User Id=$SqlLogin;Password=******;Current Language=us_english;Encrypt=True;trustServerCertificate=false"
##Copy data to DataSet
$SqlCommand = New-Object System.Data.SqlClient.SqlCommand
$SqlCommand.CommandText = $Src_SqlQuery
$SqlCommand.Connection = $Src_SqlConnection

$SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
$SqlAdapter.SelectCommand = $SqlCommand
$Src_SqlConnection.Open();
$DataSet = New-Object System.Data.DataSet
$SqlAdapter.Fill($DataSet) | Out-Null

#finally
$Src_SqlConnection.Close();$Src_SqlConnection.Dispose()

#return Dataset
Return $DataSet;
}


function ExecuteNonQuery {
    Param(  
        [string]$SQLServer,
        [string]$DBName,
        [string]$SqlLogin,
        [string]$Password,
        [string]$CommandText
    )  

##Truncate stage table
$Target_SqlConnection = New-Object System.Data.SqlClient.SqlConnection

##OnPrem connection string
$Target_SqlConnection.ConnectionString ="Data Source=$SQLServer;User Id=$SqlLogin;Password=$Password;Initial Catalog=$DBName;"
$Target_SqlConnection.Open();
$SqlCommand = New-Object System.Data.SqlClient.SqlCommand
$SqlCommand.CommandText = $CommandText;
$SqlCommand.Connection = $Target_SqlConnection;
$SqlCommand.ExecuteNonQuery();
$Target_SqlConnection.Close();
$Target_SqlConnection.Dispose();
$SqlCommand.Dispose();
}

function BulkCopyToTarget {
    Param(  
        [string] $SQLServer,
        [string] $DBName,
        [string] $TableName,
        [string] $SqlLogin,
        [string] $Password,
        [System.Data.DataSet] $DataSet
    )  

##bulk copy
$Target_SqlConnection = New-Object System.Data.SqlClient.SqlConnection
$Target_SqlConnection.ConnectionString ="Data Source=$SQLServer;User Id=$SqlLogin;Password=$Password;Initial Catalog=$DBName;"
$SqlBulkCopy = New-Object Data.SqlClient.SqlBulkCopy($Target_SqlConnection.ConnectionString, [System.Data.SqlClient.SqlBulkCopyOptions]::TableLock)
$SqlBulkCopy.DestinationTableName = $TableName
$SqlBulkCopy.WriteToServer($DataSet.Tables[0])
$Target_SqlConnection.Close()

$SqlBulkCopy.Close(); $SqlBulkCopy.Dispose() 
$DataSet.Dispose()
}

Function Get-IniContent {  
    <#  
    .Synopsis  
        Gets the content of an INI file  
          
    .Description  
        Gets the content of an INI file and returns it as a hashtable  
          
    .Notes  
        Author        : Oliver Lipkau <oliver@lipkau.net>  
        Blog        : http://oliver.lipkau.net/blog/  
        Source        : https://github.com/lipkau/PsIni 
                      http://gallery.technet.microsoft.com/scriptcenter/ea40c1ef-c856-434b-b8fb-ebd7a76e8d91 
        Version        : 1.0 - 2010/03/12 - Initial release  
                      1.1 - 2014/12/11 - Typo (Thx SLDR) 
                                         Typo (Thx Dave Stiff) 
          
        #Requires -Version 2.0  
          
    .Inputs  
        System.String  
          
    .Outputs  
        System.Collections.Hashtable  
          
    .Parameter FilePath  
        Specifies the path to the input file.  
          
    .Example  
        $FileContent = Get-IniContent "C:\myinifile.ini"  
        -----------  
        Description  
        Saves the content of the c:\myinifile.ini in a hashtable called $FileContent  
      
    .Example  
        $inifilepath | $FileContent = Get-IniContent  
        -----------  
        Description  
        Gets the content of the ini file passed through the pipe into a hashtable called $FileContent  
      
    .Example  
        C:\PS>$FileContent = Get-IniContent "c:\settings.ini"  
        C:\PS>$FileContent["Section"]["Key"]  
        -----------  
        Description  
        Returns the key "Key" of the section "Section" from the C:\settings.ini file  
          
    .Link  
        Out-IniFile  
    #>  
      
    [CmdletBinding()]  
    Param(  
        [ValidateNotNullOrEmpty()]  
        [ValidateScript({(Test-Path $_) -and ((Get-Item $_).Extension -eq ".ini")})]  
        [Parameter(ValueFromPipeline=$True,Mandatory=$True)]  
        [string]$FilePath  
    )  
      
    Begin  
        {Write-Verbose "$($MyInvocation.MyCommand.Name):: Function started"}  
          
    Process  
    {  
        Write-Verbose "$($MyInvocation.MyCommand.Name):: Processing file: $Filepath"  
              
        $ini = @{}  
        switch -regex -file $FilePath  
        {  
            "^\[(.+)\]$" # Section  
            {  
                $section = $matches[1]  
                $ini[$section] = @{}  
                $CommentCount = 0  
            }  
            "^(;.*)$" # Comment  
            {  
                if (!($section))  
                {  
                    $section = "No-Section"  
                    $ini[$section] = @{}  
                }  
                $value = $matches[1]  
                $CommentCount = $CommentCount + 1  
                $name = "Comment" + $CommentCount  
                $ini[$section][$name] = $value  
            }   
            "(.+?)\s*=\s*(.*)" # Key  
            {  
                if (!($section))  
                {  
                    $section = "No-Section"  
                    $ini[$section] = @{}  
                }  
                $name,$value = $matches[1..2]  
                $ini[$section][$name] = $value  
            }  
        }  
        Write-Verbose "$($MyInvocation.MyCommand.Name):: Finished Processing file: $FilePath"  
        Return $ini  
    }  
          
    End  
        {Write-Verbose "$($MyInvocation.MyCommand.Name):: Function ended"}  
}

##=============
# Main
##=============

[string]$global:scriptPath = split-path -parent $MyInvocation.MyCommand.Definition
[string]$global:ScriptFileName = $MyInvocation.MyCommand.Name
Set-Location $scriptPath

try
{

    #Get settings.ini
    $ini = $null
    $ini = Get-IniContent -FilePath "$scriptPath\settings.ini"
    $Src_SQLServer = $ini["Source"].Server
    $Src_DBName = $ini["Source"].Database
	$Src_TableName = $ini["Source"].TableName
    $Src_SqlLogin = $ini["Source"].SqlLogin
    $Src_SqlPassword = $ini["Source"].SqlPassword
    $Target_SQLServer = $ini["Target"].Server
    $Target_DBName = $ini["Target"].Database
	$Target_TableName = $ini["Target"].TableName
    $Target_SqlLogin = $ini["Target"].SqlLogin
    $Target_SqlPassword = $ini["Target"].SqlPassword

    #Get the Source Data
    write-host "Getting Source data..."
    $DataSet = GetSourceData -SQLServer "$Src_SQLServer" -DBName "$Src_DBName" -TableName "$Src_TableName" -SqlLogin "$Src_SqlLogin" -Password "$Src_SqlPassword"
    write-host "Getting Source data. Done!"

    #Truncate the Target table
    write-host "Delete Target data..."
    ExecuteNonQuery -SQLServer "$Target_SQLServer" -DBName "$Target_DBName" -SqlLogin "$Target_SqlLogin" -Password "$Target_SqlPassword" -CommandText "DELETE FROM $Target_TableName"
    write-host "Delete Target data. Done!"

    #Fill Target with new data
    write-host "Insert Source data to Target ..."
    BulkCopyToTarget -SQLServer "$Target_SQLServer" -DBName "$Target_DBName" -TableName "$Target_TableName" -SqlLogin "$Target_SqlLogin" -Password "$Target_SqlPassword" -DataSet $DataSet
    write-host "Insert Source data to Target. Done!"

    exit 0
}
catch [Exception]
{
    $ErrorMessage = $_.Exception.Message
	Write-Error "$ErrorMessage"
    exit 1
}
finally
{
    # Disconnect, clean up
    
}