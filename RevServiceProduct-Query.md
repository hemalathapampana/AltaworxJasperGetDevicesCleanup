## Query `RevServiceProduct`

**Goal**: Fetch all rows from `dbo.RevServiceProduct`.

### Raw SQL
```sql
SELECT *
FROM dbo.RevServiceProduct;
```

### Command line (sqlcmd)
Replace placeholders with your values.
```bash
sqlcmd -S <server> -d <database> -U <user> -P '<password>' -Q "SELECT * FROM dbo.RevServiceProduct;"
```

### C# (.NET via Microsoft.Data.SqlClient)
```csharp
using System;
using Microsoft.Data.SqlClient;

var connectionString = "Server=<server>;Database=<database>;User ID=<user>;Password=<password>;TrustServerCertificate=True;";
using var connection = new SqlConnection(connectionString);
using var command = new SqlCommand("SELECT * FROM dbo.RevServiceProduct", connection);
connection.Open();
using var reader = command.ExecuteReader();
var schema = reader.GetColumnSchema();

// Print column headers
for (int i = 0; i < schema.Count; i++)
{
    Console.Write(i == 0 ? schema[i].ColumnName : "\t" + schema[i].ColumnName);
}
Console.WriteLine();

// Print rows
while (reader.Read())
{
    for (int i = 0; i < reader.FieldCount; i++)
    {
        var value = reader.IsDBNull(i) ? "" : reader.GetValue(i)?.ToString();
        Console.Write(i == 0 ? value : "\t" + value);
    }
    Console.WriteLine();
}
```

### Notes
- If you use Windows Integrated Authentication, replace `-U/-P` with `-E` in `sqlcmd`.
- Ensure your network/firewall allows access to the SQL Server.
