using System;
using System.Collections.Generic;
using System.Data;
using Amop.Core.Models.Integration;
using Microsoft.Data.SqlClient;

namespace Amop.Core.Repositories.Integration
{
    public class IntegrationTypeRepository : IIntegrationTypeRepository
    {
        private readonly string _connectionString;

        public IntegrationTypeRepository(string connectionString)
        {
            _connectionString = connectionString;
        }
        public IList<IntegrationTypeModel> GetIntegrationTypes()
        {
            var integrationTypeList = new List<IntegrationTypeModel>();
            using (var conn = new SqlConnection(_connectionString))
            {
                conn.Open();
                var sqlCommandText = "Select id, Name FROM Integration";
                using (var cmd = new SqlCommand(sqlCommandText, conn))
                {
                    cmd.CommandType = CommandType.Text;
                    using (var rdr = cmd.ExecuteReader())
                    {
                        while (rdr.Read())
                        {
                            integrationTypeList.Add(ReadIntegrationType(rdr));
                        }
                    }
                }
            }

            return integrationTypeList;
        }

        private static IntegrationTypeModel ReadIntegrationType(IDataRecord data)
        {
            return new IntegrationTypeModel
            {
                Id = Convert.ToInt32(data["id"].ToString()),
                Name = data["Name"].ToString()
            };
        }
    }
}
