using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;



class Program
{
    static async System.Threading.Tasks.Task Main(string[] args)
    {


        // Dados de autenticação da API
        string apiKey = "UO86dUKpK6RuOikxPkXA5aQSdLXK3xGq";

        // Configuração da requisição para a API
        var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.Add("x-api-key", apiKey);

        // Faz a primeira chamada para obter o número total de itens
        var initialContent = new StringContent(JsonConvert.SerializeObject(new { limit = 1, page = 1 }), Encoding.UTF8, "application/json");
        var initialResponse = await httpClient.PostAsync("https://api.contabilone.com/invoices", initialContent);
        var initialResponseContent = await initialResponse.Content.ReadAsStringAsync();
        dynamic initialParsedResponse = JsonConvert.DeserializeObject(initialResponseContent);
        int totalItems = initialParsedResponse.totalItems;

        // Define o limite de itens por página
        int limit = 1000;

        // Calcula o número total de páginas
        int totalPages = (int)Math.Round((double)totalItems / ((limit)*1000), MidpointRounding.AwayFromZero);
        
        bool shouldExit = false;
        // Realiza uma requisição para cada página

        int totalPagesProcessed = 0; // Variável para contar o número de páginas processadas


        for (int page = 1; page <= totalPages; page++)
        {
            bool requestSuccessful = false;

            while (!requestSuccessful)
            {
                

                
                
                
                    try
                    {

                        // Requisição para a página atual
                        var requestPayload = new
                        {
                            limit = limit,
                            page = page,
                            filters = new[]
                            {
                            new
                            {
                                name = "ide.dhEmi",
                                type = "range",
                                values = new[]
                                {
                                    new
                                    {
                                        start = "2023-06-15T00:00:00",
                                        end = "2023-06-15T07:59:59"
                                    }
                                }
                            }
                        }
                        };



                        var content = new StringContent(JsonConvert.SerializeObject(requestPayload), Encoding.UTF8, "application/json");

                        var response = await httpClient.PostAsync("https://api.contabilone.com/invoices", content);
                        var responseContent = await response.Content.ReadAsStringAsync();

                        // Faz o parse da resposta da API
                        dynamic parsedResponse = JsonConvert.DeserializeObject(responseContent);

                        // Extrai os dados relevantes da resposta
                        List<InvoiceData> invoiceDataList = new List<InvoiceData>();
                        if (parsedResponse != null && parsedResponse.message != null && parsedResponse.message.Count > 0)
                        {
                            foreach (var item in parsedResponse.message)
                            {
                                if (item?.emit != null && item?.createdAt != null && item?.ide != null && item?.events?.authorization != null)
                                {
                                    var invoiceData = new InvoiceData
                                    {
                                        CNPJ = item?.emit?.CNPJ ?? string.Empty,
                                        InscricaoEstadual = item?.emit?.IE ?? string.Empty,
                                        Nome = item?.emit?.xFant ?? string.Empty,
                                        RequestId = item.requestId ?? string.Empty,
                                        RemoteId = item?.events?.authorization?.remoteId ?? string.Empty,
                                        DataProcessamento = item?.createdAt?.formatted ?? string.Empty,
                                        Modelo = item?.ide?.mod ?? string.Empty,
                                        ChaveAcesso = item?.chNFe?.ToString() ?? string.Empty,
                                        Status = item?.events?.authorization?.status ?? string.Empty,
                                        vNF = item?.vNF?.ToString() ?? string.Empty
                                    };

                                    invoiceDataList.Add(invoiceData);
                                }
                                // Processa os dados encontrados
                                // ...

                            }

                            requestSuccessful = true; // Indica que a requisição foi bem-sucedida


                        }



                    requestSuccessful = true;


                    // Conexão com o banco de dados Oracle SQL

                    var connectionString = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.3.201.137)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=RJD14DSV)));User ID=SYNCHRO;Password=synchro;";


                        using (var connection = new OracleConnection(connectionString))
                        {
                            connection.Open();

                            foreach (var invoiceData in invoiceDataList)
                            {
                                try
                                {
                                    using (var command = new OracleCommand("DELETE FROM synchro.requisicao_sefaz WHERE CHAVE_DE_ACESSO = :chaveAcesso", connection))
                                    {
                                        command.Parameters.Add("chaveAcesso", invoiceData.ChaveAcesso);
                                        command.ExecuteNonQuery();
                                    }

                                    using (var command = new OracleCommand("INSERT INTO synchro.requisicao_sefaz (CNPJ, INSCRIÇÃO_ESTADUAL, NOME, REQUEST_ID, REMOTE_ID, DATA_DO_PROCESSAMENTO, MODELO, CHAVE_DE_ACESSO, STATUS, VNF) " +
                                                                          "VALUES (:cnpj, :inscricaoEstadual, :nome, :requestId, :remoteId, :dataProcessamento, :modelo, :chaveAcesso, :status, :vNF)", connection))

                                    {

                                        command.Parameters.Add("cnpj", invoiceData.CNPJ);
                                        command.Parameters.Add("inscricaoEstadual", invoiceData.InscricaoEstadual);
                                        command.Parameters.Add("nome", invoiceData.Nome);
                                        command.Parameters.Add("requestId", invoiceData.RequestId);
                                        command.Parameters.Add("remoteId", invoiceData.RemoteId);
                                        command.Parameters.Add("dataProcessamento", DateTime.ParseExact(invoiceData.DataProcessamento, "dd/MM/yyyy - HH:mm:ss", CultureInfo.InvariantCulture));
                                        command.Parameters.Add("modelo", invoiceData.Modelo);
                                        command.Parameters.Add("chaveAcesso", invoiceData.ChaveAcesso);
                                        command.Parameters.Add("status", invoiceData.Status);
                                        command.Parameters.Add("vNF", Convert.ToDecimal(invoiceData.vNF, CultureInfo.GetCultureInfo("pt-BR")));

                                        command.ExecuteNonQuery();
                                        Console.WriteLine("Processo executado com sucesso: " + invoiceData.ChaveAcesso);




                                    }

                                }
                                catch (System.AccessViolationException ex)
                                {
                                    Console.WriteLine("Erro de acesso à memória protegida: " + ex.Message);
                                    // Reduz o contador da página para repetir a mesma página
                                    page--;
                                    break; // Sai do loop atual
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("Ocorreu um erro ao processar os dados: " + ex.Message);
                                    // Reduz o contador da página para repetir a mesma página
                                    page--;
                                    break; // Sai do loop atual
                                }
                            }

                            connection.Close();
                        }



                        Console.WriteLine("Processo executado com sucesso na página: " + page +" Total de paginas: "+ totalPages);
                        // Pausa a execução por 3 minutos
                        //await Task.Delay(TimeSpan.FromMinutes(3));




                        

                    }



                    catch (System.AccessViolationException ex)
                    {
                        Console.WriteLine("Ocorreu um erro de Access Violation Exception na página " + page + ": " + ex.Message);
                        Console.WriteLine("Aguardando 3 minutos antes de tentar novamente...");

                        // Espera 3 minutos antes de tentar novamente
                        await Task.Delay(TimeSpan.FromMinutes(3));

                        // Reduz o contador da página para repetir a mesma página
                        page--;
                    }

                
            }

                

            
        }


        

    }
   
}







class InvoiceData
{
    public string CNPJ { get; set; }
    public string InscricaoEstadual { get; set; }
    public string Nome { get; set; }
    public string RemoteId { get; set; }
    public string RequestId { get; set; }
    public string DataProcessamento { get; set; }
    public string Modelo { get; set; }
    public string ChaveAcesso { get; set; }
    public string Status { get; set; }
    public string vNF { get; set; } 
}