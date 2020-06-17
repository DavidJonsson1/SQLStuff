function init{

    #Get settings.ini
    $ini = $null
    $ini = Get-IniContent -FilePath "$scriptPath\settings.ini"
    $global:Src_sourceFile = $ini["Source"].sourceFile
    $global:Target_SQLServer = $ini["Target"].Server
    $global:Target_DBName = $ini["Target"].Database
    $global:Target_TableName = $ini["Target"].TableName
    $global:Target_SqlLogin = $ini["Target"].SqlLogin
    $global:Target_SqlPassword = $ini["Target"].SqlPassword

    $global:targetConnectionString = GetConnectionString -SQLServer $Target_SQLServer -DBName $Target_DBName -SqlLogin $Target_SqlLogin -Password $Target_SqlPassword

}

function createDataTable($columns)
{
    ###Creating a new DataTable###
    $tempTable = New-Object System.Data.DataTable
   
    ##Creating Columns for DataTable##
    foreach($column in $columns) {
        $col = New-Object System.Data.DataColumn(“$column”)
        $tempTable.columns.Add($col)
    }
    Write-Output -NoEnumerate $tempTable
}

function BulkCopyToTarget {
    Param(  
        [string] $ConnectionString,
        [string] $TableName,
        [System.Data.DataSet] $DataSet
    )  

##bulk copy
$SqlConnection = New-Object System.Data.SqlClient.SqlConnection
$SqlConnection.ConnectionString = $ConnectionString
$SqlBulkCopy = New-Object Data.SqlClient.SqlBulkCopy($SqlConnection.ConnectionString, [System.Data.SqlClient.SqlBulkCopyOptions]::TableLock)
$SqlBulkCopy.DestinationTableName = $TableName
$SqlBulkCopy.WriteToServer($DataSet.Tables[0])
$SqlConnection.Close()

$SqlBulkCopy.Close(); $SqlBulkCopy.Dispose() 
$DataSet.Dispose()
}

function GetConnectionString{
    Param(  
        [string]$SQLServer,
        [string]$DBName,
        [string]$SqlLogin,
        [string]$Password
        )

    if ($SQLServer -contains "database.windows.net") {
        $isAzure = $true
    }

    if ($SqlLogin -eq "WinAuth") {
        $WinAuth = $true
    }


    ##Azure
    if ($isAzure -eq $true) {
        $connectionString = "Data Source=tcp:$SQLServer;Initial Catalog=$DBName;User Id=$SqlLogin;Password=$Password;Current Language=us_english;Encrypt=True;trustServerCertificate=false"
    }
    else {
        if ($WinAuth -eq $true) {
        $connectionString = "Data Source=$SQLServer;Initial Catalog=$DBName;Integrated Security = SSPI"
        } else {
        $connectionString = "Data Source=$SQLServer;Initial Catalog=$DBName;User Id=$SqlLogin;Password=$Password"
        }
    }

return $connectionString
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

function csv-Load {
Param
    (
        $sourceFile
    )

    $CSVDataTable = Import-Csv $sourceFile
    $columns = $CSVDataTablepsobject.properties.name
    $table = createDataTable -columns $columns
    $table = $CSVDataTable | Out-DataTable
    return ,$table
}

function Get-Type
{ 
    param($type) 
 
$types = @( 
'System.Boolean', 
'System.Byte[]', 
'System.Byte', 
'System.Char', 
'System.Datetime', 
'System.Decimal', 
'System.Double', 
'System.Guid', 
'System.Int16', 
'System.Int32', 
'System.Int64', 
'System.Single', 
'System.UInt16', 
'System.UInt32', 
'System.UInt64') 
 
    if ( $types -contains $type ) { 
        Write-Output "$type" 
    } 
    else { 
        Write-Output 'System.String' 
         
    } 
}
 
function Out-DataTable 
{ 
    [CmdletBinding()] 
    param([Parameter(Position=0, Mandatory=$true, ValueFromPipeline = $true)] [PSObject[]]$InputObject) 
 
    Begin 
    { 
        $dt = new-object Data.datatable   
        $First = $true  
    } 
    Process 
    { 
        foreach ($object in $InputObject) 
        { 
            $DR = $DT.NewRow()   
            foreach($property in $object.PsObject.get_properties()) 
            {   
                if ($first) 
                {   
                    $Col =  new-object Data.DataColumn   
                    $Col.ColumnName = $property.Name.ToString()   
                    if ($property.value) 
                    { 
                        if ($property.value -isnot [System.DBNull]) { 
                            $Col.DataType = [System.Type]::GetType("$(Get-Type $property.TypeNameOfValue)") 
                         } 
                    } 
                    $DT.Columns.Add($Col) 
                }   
                if ($property.Gettype().IsArray) { 
                    $DR.Item($property.Name) =$property.value | ConvertTo-XML -AS String -NoTypeInformation -Depth 1 
                }   
               else { 
                    $DR.Item($property.Name) = $property.value 
                } 
            }   
            $DT.Rows.Add($DR)   
            $First = $false 
        } 
    }  
      
    End 
    { 
        Write-Output @(,($dt)) 
    } 
 
}
##==========
##Entry point
##==========
[string]$global:scriptPath = split-path -parent $MyInvocation.MyCommand.Definition
[string]$global:ScriptFileName = $MyInvocation.MyCommand.Name
Set-Location $scriptPath

try
{
    init

    $sourceFile = "$scriptPath\$Src_sourceFile"

    write-host "start ..."
    $table = csv-Load -sourceFile $sourceFile

    $dataset = New-Object System.Data.DataSet
    $dataset.Tables.Add($table)

    BulkCopyToTarget -ConnectionString $targetConnectionString -TableName $Target_TableName -DataSet $dataset

    write-host "Done!"
}
catch
{
  write-host $error[0]
  echo $_.Exception | format-list -force
  exit 1
}