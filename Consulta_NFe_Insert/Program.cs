using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System.ServiceProcess;
using System.Threading;

namespace Upload_NFe_Cone
{
    class Program
    {
        static void Main(string[] args)
        {
            if (Environment.UserInteractive)
            {
                // Run as a console application
                Console.WriteLine("Executando pelo console do aplicativo...");
                var service = new InvoiceService();
                service.ProcessInvoiceData(CancellationToken.None).Wait();
                Console.WriteLine("Pressione qualquer tecla para sair...");
                Console.ReadKey();
            }
            else
            {
                // Executar como serviço do Windows
                ServiceBase[] servicesToRun = new ServiceBase[]
                {
                    new InvoiceService()
                };
                ServiceBase.Run(servicesToRun);
            }
        }
    }

    public class InvoiceService : ServiceBase
    {
       /// private System.Timers.Timer timer;
        //private DateTime lastExecutionTime;
        private CancellationTokenSource cancellationTokenSource;

      ///  private bool isProcessingCompleted = false;
        public InvoiceService()
        {
           // ServiceName = "InvoiceService";
            //lastExecutionTime = DateTime.Now.AddMinutes(-1); // Define lastExecutionTime para 1 minuto antes do início do serviço
            cancellationTokenSource = new CancellationTokenSource();

            // Iniciando timer
          ///  timer = new System.Timers.Timer();
        }

        protected override void OnStart(string[] args)
        {
            
            // Calcula o intervalo para a próxima execução (24 horas após o término da execução anterior)
            //TimeSpan timeToNextExecution = lastExecutionTime.AddHours(24) - DateTime.Now;

           // timer.Interval = timeToNextExecution.TotalMilliseconds;
           // timer.Elapsed += Timer_Elapsed;
          //  timer.Start();

            // Iniciar o processamento imediatamente no início do serviço
            Task.Run(() => ProcessInvoiceData(cancellationTokenSource.Token));

        }

        protected override void OnStop()
        {
            Stop();
            Dispose();
        }

        //private async void Timer_Elapsed(object sender, ElapsedEventArgs e)
        //{
        //    // Recalcula o intervalo para a próxima execução (24 horas após o término da execução anterior)
        //    //timer.Interval = TimeSpan.FromDays(1).TotalMilliseconds;

        //    // Se o processamento não estiver concluído, não inicia uma nova execução.
        //    if (!isProcessingCompleted)
        //    {
        //        return;
        //    }

        //    // Atualiza lastExecutionTime para a data e hora atuais
        //   // lastExecutionTime = DateTime.Now;

        //    // Marca o processamento como não concluído para evitar que uma nova execução seja iniciada imediatamente.
        //    isProcessingCompleted = false;

        //    // Executar a lógica aqui, você pode chamar ProcessInvoiceData diretamente.
        //   // await ProcessInvoiceData(cancellationTokenSource.Token);

        //    // Marca o processamento como concluído após a execução de todas as páginas.
        //    isProcessingCompleted = true;
        //}


        public async Task ProcessInvoiceData(CancellationToken cancellationToken)
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
            int limit = 100;

            // Calcula o número total de páginas --->> quando a pagina for multipla de 100 sleep 1 minuto
            int totalPages = (int)Math.Ceiling((double)totalItems / limit);

            bool shouldExit = false;

            // Realiza uma requisição para cada página
            int totalPagesProcessed = 0; // Variável para contar o número de páginas processadas

            for (int page = 1; page <= totalPages; page++)
            {
                if(page%100 == 0)
                    Thread.Sleep(60000);

                if (cancellationToken.IsCancellationRequested)
                {
                    Console.WriteLine("Task has been canceled.");
                    break;
                }

                bool requestSuccessful = false;

                while (!requestSuccessful)
                {
                    try
                    {
                        // Requisição para a página atual
                       var startDate = DateTime.Now.AddDays(-20).ToString("yyyy-MM-dd") + "T00:00:00";
                       var endDate = DateTime.Now.ToString("yyyy-MM-dd") + "T23:59:59";

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
                                start = startDate ,///"2023-07-31T00:00:00", ///
                                end =  endDate ///"2023-07-31T12:59:59" ///
                            }
                        }
                    }
                }
                        };

                        var content = new StringContent(JsonConvert.SerializeObject(requestPayload), Encoding.UTF8, "application/json");
                        var response = await httpClient.PostAsync("https://api.contabilone.com/invoices", content);
                        var responseContent = await response.Content.ReadAsStringAsync();

                        // Verifica se a resposta contém uma mensagem de erro
                        if (responseContent.Contains("The api client token is making too many requests"))
                        {
                            Console.WriteLine("A API retornou um erro: The api client token is making too many requests. Aguardando 1 minuto antes de tentar novamente...");
                            await Task.Delay(TimeSpan.FromMinutes(3)); // Aguarda 1 minuto antes de tentar novamente

                            // Opcionalmente, você pode adicionar uma lógica para registrar o erro em um log ou tomar outras ações apropriadas.
                        }
                        else
                        {

                            // Faz o parse da resposta da API
                            dynamic parsedResponse = JsonConvert.DeserializeObject(responseContent);

                            // Atualiza o valor de totalItems caso haja alterações
                            if (parsedResponse != null && parsedResponse.totalItems != null)
                            {
                                totalItems = parsedResponse.totalItems;
                                totalPages = (int)Math.Ceiling((double)totalItems / limit);
                            }


                            // Extrai os dados relevantes da resposta
                            List<InvoiceData> invoiceDataList = new List<InvoiceData>();
                            if (parsedResponse != null && parsedResponse.message != null)
                            {
                                IEnumerable<dynamic> messageItems = parsedResponse.message.ToObject<IEnumerable<dynamic>>();
                                int itemCount = (messageItems as ICollection<dynamic>)?.Count ?? 0;

                                foreach (var item in messageItems)

                                {
                                    if (item?.emit != null && item?.createdAt != null && item?.ide != null && item?.events?.authorization != null && item?.chNFe != null)
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
                                            vNF = item?.vNF != null ? item.vNF.ToString("0.00") : "0.00"
                                        };

                                        invoiceDataList.Add(invoiceData);
                                    }
                                    // Processa os dados encontrados
                                    // ...

                                }

                                requestSuccessful = true; // Indica que a requisição foi bem-sucedida


                            }




                            requestSuccessful = true;  // Indica que a requisição foi bem-sucedida


                            // Conexão com o banco de dados Oracle SQL

                            var connectionString = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.3.201.138)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=RJD14)));User ID=INTEGRA;Password=Int3gr4DUF;";


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
                                            decimal vNFValue = Convert.ToDecimal(invoiceData.vNF, CultureInfo.GetCultureInfo("pt-BR"));
                                            decimal vNFValueInDatabase = vNFValue / 100;

                                            command.Parameters.Add("vNF", vNFValueInDatabase);

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



                            Console.WriteLine("Processo executado com sucesso na página: " + page + " Total de paginas: " + totalPages);
                            // Pausa a execução por 3 minutos
                            //await Task.Delay(TimeSpan.FromMinutes(3));




                        }

                    }



                    catch (System.AccessViolationException ex)
                    {
                        //await Task.Delay(TimeSpan.FromMinutes(1)); // Aguarda 1 minuto (pode ser ajustado conforme necessário)

                        Console.WriteLine("Ocorreu um erro de Access Violation Exception na página " + page + ": " + ex.Message);
                        Console.WriteLine("Aguardando 3 minutos antes de tentar novamente...");

                        // Espera 3 minutos antes de tentar novamente
                        await Task.Delay(TimeSpan.FromMinutes(3));

                        // Reduz o contador da página para repetir a mesma página
                        page--;
                        /// currentAttempt++;

                    }


                }


                if (!requestSuccessful)
                {
                    // Caso todas as tentativas tenham falhado, você pode decidir como tratar essa situação.
                    // Pode ser uma boa ideia registrar o erro em um arquivo de log ou notificar o administrador do sistema.
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
}