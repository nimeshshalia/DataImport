using CsvHelper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using DataImport.Models;

namespace DataImport.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;
        private readonly string _connectionString;
        public HomeController(IConfiguration configuration)
        {
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }
        public IActionResult Index()
        {
            return View();
        }

        public IActionResult Privacy()
        {
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }

        [HttpPost]
        public async Task<IActionResult> UploadCsv(IFormFile csvFile)
        {
            if (csvFile == null || csvFile.Length == 0)
            {
                return BadRequest("No file uploaded.");
            }

            DateTime now = DateTime.Now;
            var csvConfigFile = "CSV_Config_" + now.ToString("dd_MM_yyyy");
            var stagingTableName = "Staging_" + now.ToString("dd_MM_yyyy");

            try
            {
                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    string dropTablesQuery =
                        $"DROP TABLE IF EXISTS dbo.{csvConfigFile}; " +
                        $"DROP TABLE IF EXISTS dbo.{stagingTableName}; ";

                    using (SqlCommand command = new SqlCommand(dropTablesQuery, connection))
                    {
                        // Open the connection to the database.
                        connection.Open();

                        // Execute the non-query command (like DROP, CREATE, UPDATE, DELETE).
                        command.ExecuteNonQuery();

                        // The code has executed successfully.
                        Console.WriteLine($"Table '{csvConfigFile}' dropped successfully (if it existed).");
                    }
                }
            }
            catch (SqlException ex)
            {
                // Log or handle any SQL-specific errors.
                Console.WriteLine("An error occurred with the database operation.");
                Console.WriteLine(ex.Message);
            }
            catch (Exception ex)
            {
                // Catch other general exceptions.
                Console.WriteLine("An unexpected error occurred.");
                Console.WriteLine(ex.Message);
            }

            using (var reader = new StreamReader(csvFile.OpenReadStream()))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                // Read CSV headers to determine column names and potential types
                csv.Read();
                csv.ReadHeader();
                var headers = csv.HeaderRecord;

                var createTableSql = $"CREATE TABLE {csvConfigFile} (";
                foreach (var header in headers)
                {
                    createTableSql += $"[{header}] NVARCHAR(MAX),";
                }
                createTableSql = createTableSql.TrimEnd(',') + ")";

                // Execute CREATE TABLE statement
                using (var connection = new SqlConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    using (var command = new SqlCommand(createTableSql, connection))
                    {
                        await command.ExecuteNonQueryAsync();
                    }

                    // Bulk insert data into the newly created table
                    using (var bulkCopy = new SqlBulkCopy(connection))
                    {
                        bulkCopy.DestinationTableName = csvConfigFile;

                        // Create a DataTable from CSV data
                        var dataTable = new DataTable();
                        foreach (var header in headers)
                        {
                            dataTable.Columns.Add(header);
                        }

                        while (csv.Read())
                        {
                            var row = dataTable.NewRow();
                            foreach (var header in headers)
                            {
                                row[header] = csv.GetField(header);
                            }
                            dataTable.Rows.Add(row);
                        }
                        await bulkCopy.WriteToServerAsync(dataTable);
                    }
                }
                await CreateStagingTable(csvConfigFile, stagingTableName);
            }

            return Ok("CSV Configuration table has been created successfully.");
        }
        private async Task CreateStagingTable(string configTable, string stagingTable)
        {

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();

                var cmd = new SqlCommand($"SELECT * FROM {configTable}", connection);
                var reader = await cmd.ExecuteReaderAsync();

                var schema = reader.GetColumnSchema();

                string colNameCol = schema.First(c => c.ColumnName.ToLower().Contains("name")).ColumnName;
                string colTypeCol = schema.First(c => c.ColumnName.ToLower().Contains("type")).ColumnName;
                string colNullCol = schema.First(c => c.ColumnName.ToLower().Contains("null")).ColumnName;

                var sb = new StringBuilder();

                while (await reader.ReadAsync())
                {
                    string columnName = reader[colNameCol].ToString();
                    string dataType = reader[colTypeCol].ToString();
                    string allowNull = reader[colNullCol].ToString();

                    sb.Append($"[{columnName}] {dataType} ");
                    sb.Append(allowNull.Equals("No", StringComparison.OrdinalIgnoreCase) ? "NOT NULL, " : "NULL, ");
                }

                reader.Close();
                string columnDefinition = sb.ToString().TrimEnd(' ', ',');

                var spCmd = new SqlCommand("sp_CreateTableFromDefinition", connection);
                spCmd.CommandType = CommandType.StoredProcedure;
                spCmd.Parameters.AddWithValue("@TableName", stagingTable);
                spCmd.Parameters.AddWithValue("@ColumnsDefinition", columnDefinition);

                await spCmd.ExecuteNonQueryAsync();
            }
        }


        [HttpPost]
        public async Task<IActionResult> UploadCsvData(IFormFile file) // Working Example Parsing Json + Stored procedure
        {
            DateTime now = DateTime.Now;
            var stagingTableName = "Staging_" + now.ToString("dd_MM_yyyy");
            var keyColumn = "Email";
            try
            {
                if (file == null || file.Length == 0)
                    return BadRequest("Invalid file");

                var records = new List<Dictionary<string, object>>();

                // CsvHelper configuration for reading files
                //var config = new CsvConfiguration(CultureInfo.InvariantCulture)
                //{
                //    HasHeaderRecord = true, // We need the header record for dynamic columns
                //    Delimiter = ","         // Assuming standard comma delimiter
                //};
                using (var reader = new StreamReader(file.OpenReadStream()))
                using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
                {
                    // 1. Read the header record
                    if (!csv.Read() || !csv.ReadHeader())
                    {
                        throw new InvalidOperationException("CSV file must contain a header row.");
                    }

                    // 2. Iterate through data records
                    while (csv.Read())
                    {
                        var rowData = new Dictionary<string, object>();

                        // 3. Dynamically read each field using the header names
                        foreach (var header in csv.HeaderRecord)
                        {
                            rowData[header] = csv.GetField(header);
                        }
                        records.Add(rowData);
                    }
                }

                // ----------------------------------------------------------
                // 🔥 REMOVE DUPLICATES IN C# BASED ON DYNAMIC KEY COLUMN
                // ----------------------------------------------------------
                if (!records.First().ContainsKey(keyColumn))
                    return BadRequest($"Key column '{keyColumn}' not found in CSV header.");

                var uniqueRows = records
                    .Where(r => r[keyColumn] != null)                                      // prevent null key
                    .GroupBy(r => r[keyColumn]?.ToString().Trim(), StringComparer.OrdinalIgnoreCase)
                    .Select(g => g.Last())                                                // keep last occurrence
                    .ToList();

                // 4. Serialize the list of dictionaries into a JSON array string
                // The keys of the dictionary (column names) become the JSON property names.
                string jsonString = JsonSerializer.Serialize(uniqueRows);

                // Ensure you are using the dynamic SP previously defined: dbo.Import_Dynamic_JSON_Columns
                const string storedProcedureName = "dbo.Import_Dynamic_JSON_Columns";

                using (var connection = new SqlConnection(_connectionString))
                {
                    await connection.OpenAsync();

                    using (var command = new SqlCommand(storedProcedureName, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;

                        // Pass the parameters
                        command.Parameters.AddWithValue("@TableName", stagingTableName);
                        //command.Parameters.AddWithValue("@KeyColumn", keyColumn);
                        var jsonParam = command.Parameters.Add("@JsonData", System.Data.SqlDbType.NVarChar, -1);
                        jsonParam.Value = jsonString;

                        await command.ExecuteNonQueryAsync();
                    }
                }
                return Ok("CSV imported successfully!");
            }
            catch (Exception ex)
            {
                // Log Exception
                // 1. Prepare the dynamic error message securely
                string innerExMessage = ex.InnerException?.Message ?? ex.Message; // Safely get inner or outer message
                string strErrorLog = $" Line Number : {((Microsoft.Data.SqlClient.SqlException)ex).LineNumber} Column validation failed From {stagingTableName} - Inner Error: {innerExMessage} ";

                // 2. Define the query using placeholders (@ErrorMsg)
                string errorLogQry =
                    "INSERT INTO CsvImportErrors (RowNumber, ErrorMessage, LogDate) VALUES (@Row, @ErrorMsg, GETDATE())";

                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    using (SqlCommand command = new SqlCommand(errorLogQry, connection))
                    {
                        // 3. Add parameters instead of concatenating strings
                        command.Parameters.AddWithValue("@Row", 1);
                        command.Parameters.AddWithValue("@ErrorMsg", strErrorLog);

                        // Open the connection and execute
                        connection.Open();
                        command.ExecuteNonQuery();
                    }
                }
                return Ok("Error occured... Please check Error Log Table CsvImportErrors");
                //throw ex;
            }
        }
    }
}
