using FluxoCaixa.Integracoes.Extensions;
using FluxoCaixa.Integracoes.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.OpenApi.Models;
using System.Globalization;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowAll",
        builder => builder
            .AllowAnyOrigin()
            .AllowAnyMethod()
            .AllowAnyHeader());
});
// 🔹 Registrar Interfaces e Implementações
builder.Services.AddSingleton<ICustomEnvironment, CustomEnvironment>();
builder.Services.AddSingleton<IDynamoDbService, DynamoDbService>();
builder.Services.AddSingleton<IRabbitMqConsumer, RabbitMqConsumer>();

// 🔹 Configuração do Swagger
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new OpenApiInfo { Title = "Fluxo de Caixa-Integrações API", Version = "v1" });
});

var app = builder.Build();

app.UseCors(policy=>policy.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader());
// 🔹 Obtém o serviço injetado para verificar ambiente
var env = app.Services.GetRequiredService<ICustomEnvironment>();

// 🔹 Configuração do Swagger se estiver rodando localmente
if (env.IsLocal() || env.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "Fluxo de Caixa-Integrações API V1");
        c.RoutePrefix = "";  // 🔹 Deixa o Swagger disponível na raiz "/"
    });
}

// 🔹 Obtém a instância do consumidor RabbitMQ e inicia o consumo
var rabbitMqConsumer = app.Services.GetRequiredService<IRabbitMqConsumer>();
await Task.Run(() => rabbitMqConsumer.StartConsuming());

// 🔹 Obtém a instância do serviço DynamoDB
var dynamoDbService = app.Services.GetRequiredService<IDynamoDbService>();

// Health Check Endpoint
app.MapGet("/health", (IDynamoDbService dynamoDbService, IRabbitMqConsumer rabbitMqProducer) =>
{
    try
    {
        // 🔹 Verifica conexão com DynamoDB
        var dynamoCheck = dynamoDbService != null;

        // 🔹 Verifica conexão com RabbitMQ
        var rabbitCheck = rabbitMqProducer != null;

        if (dynamoCheck && rabbitCheck)
        {
            return Results.Ok(new { status = "OK", dynamoDb = "Online", rabbitMq = "Online" });
        }
        else
        {
            return Results.Problem("Algum serviço não está disponível", statusCode: 503);
        }
    }
    catch (Exception ex)
    {
        return Results.Problem($"Erro no health check: {ex.Message}", statusCode: 500);
    }
});
// 🔹 Endpoint para reprocessar consolidado por dia, período ou tudo
app.MapPost("/integracoes/reprocessar", async ([FromQuery] string? dataInicio, [FromQuery] string? dataFim) =>
{
    try
    {
        DateTime? inicio = null, fim = null;

        if (!string.IsNullOrEmpty(dataInicio))
        {
            if (DateTime.TryParseExact(dataInicio, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsedInicio))
            {
                inicio = parsedInicio.Date; // 🔹 Garante que seja exatamente 00:00:00
            }
            else
            {
                return Results.BadRequest(new { Erro = "Formato de data inválido. Use 'yyyy-MM-dd'.", ValorRecebido = dataInicio });
            }
        }

        if (!string.IsNullOrEmpty(dataFim))
        {
            if (DateTime.TryParseExact(dataFim, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsedFim))
            {
                fim = parsedFim.Date.AddHours(23).AddMinutes(59).AddSeconds(59); // 🔹 Garante que seja até 23:59:59
            }
            else
            {
                return Results.BadRequest(new { Erro = "Formato de data inválido. Use 'yyyy-MM-dd'.", ValorRecebido = dataFim });
            }
        }

        // 🔹 Regra: A data inicial não pode ser maior que a data final
        if (inicio.HasValue && fim.HasValue && inicio > fim)
        {
            return Results.BadRequest(new { Erro = "A data inicial não pode ser maior que a data final.", DataInicio = inicio, DataFim = fim });
        }

        await dynamoDbService.ReprocessarConsolidado(inicio, fim);
        return Results.Ok(new { Mensagem = "Reprocessamento iniciado", Inicio = inicio?.ToString("yyyy-MM-dd HH:mm:ss"), Fim = fim?.ToString("yyyy-MM-dd HH:mm:ss") });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { Erro = "Falha ao iniciar o reprocessamento", Detalhes = ex.Message });
    }
});

// 🔹 Adicionar o loop para reprocessamento a cada 5 segundos
async Task ReprocessarPeriodicamente()
{
    while (true)
    {
        try
        {
            // Chama o reprocessamento a cada 5 segundos
            await app.Services.GetRequiredService<IDynamoDbService>().ReprocessarConsolidado(null, null);
            Console.WriteLine("Reprocessamento executado a cada 5 segundos...");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Erro no reprocessamento: {ex.Message}");
        }
        
        // Aguarda 5 segundos antes de rodar o próximo ciclo
        await Task.Delay(5000);
    }
}

// Inicia o loop de reprocessamento
_ = Task.Run(ReprocessarPeriodicamente);

// 🔹 Executa a aplicação
await app.RunAsync();

record MensagemFila(Guid Id, string Conteudo);
public partial class Program { }
