using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace S3PermissionTest.Controllers;

[ApiController]
[Route("[controller]/[action]")]
public class WeatherForecastController : ControllerBase
{
    private readonly ILogger<WeatherForecastController> _logger;
    private readonly S3Settings _s3Settings;
    
    private const string FileKey = "PERMISSION-TEST/file.txt";

    public WeatherForecastController(ILogger<WeatherForecastController> logger, IOptions<S3Settings> s3Settings
    )
    {
        _logger = logger;
        _s3Settings = s3Settings.Value;
    }

    private IAmazonS3 GetS3Client()
    {
        if (string.IsNullOrEmpty(_s3Settings.AccessKeyId))
        {
            return new AmazonS3Client();
        }

        return new AmazonS3Client(
            _s3Settings.AccessKeyId,
            _s3Settings.SecretAccessKey,
            RegionEndpoint.GetBySystemName(_s3Settings.Region));
    }

    [HttpGet(Name = "GetWeatherForecast")]
    public async Task<List<string>> Get()
    {
        var client = GetS3Client();
        
        ListObjectsV2Request request = new ListObjectsV2Request
        {
            BucketName = _s3Settings.BucketName,
            Prefix = "PERMISSION-TEST",
        };
        
        ListObjectsV2Response response;

        List<string> output = new ();
        
        do
        {
            response = await client.ListObjectsV2Async(request);

            // Process the response.
            foreach (S3Object entry in response.S3Objects)
            {
                output.Add($"key = {entry.Key} size = {entry.Size}");
            }
            
            Console.WriteLine("Next Continuation Token: {0}", response.NextContinuationToken);
            request.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated);

        return output;
    }

    [HttpGet(Name = "Upload")]
    public async Task<string> Upload()
    {
        try
        {
            var client = GetS3Client();

            var uploadRequest = new PutObjectRequest
            {
                BucketName = _s3Settings.BucketName,
                Key = FileKey,
                ContentBody = "sample text"
            };

            PutObjectResponse response = await client.PutObjectAsync(uploadRequest);

            return response.HttpStatusCode.ToString();
        }
        catch (Exception ex)
        {
            return ex.ToString();
        }
    }
    
    [HttpGet(Name = "Copy")]
    public async Task<string> Copy()
    {
        try
        {
            var client = GetS3Client();

            CopyObjectRequest request = new CopyObjectRequest
            {
                SourceBucket = _s3Settings.BucketName,
                SourceKey = FileKey,
                DestinationBucket = _s3Settings.BucketName,
                DestinationKey = "PERMISSION-TEST/file-copied.txt"
            };
            
            CopyObjectResponse response = await client.CopyObjectAsync(request);

            return response.HttpStatusCode.ToString();
        }
        catch (Exception ex)
        {
            return ex.ToString();
        }
    }
    
    [HttpGet(Name = "Download")]
    public async Task<string> Download()
    {
        try
        {
            var client = GetS3Client();

            GetObjectRequest request = new GetObjectRequest
            {
                BucketName = _s3Settings.BucketName,
                Key = FileKey
            };
            using (GetObjectResponse response = await client.GetObjectAsync(request))
            using (Stream responseStream = response.ResponseStream)
            using (StreamReader reader = new StreamReader(responseStream))
            {
                string title = response.Metadata["x-amz-meta-title"]; // Assume you have "title" as medata added to the object.
                string contentType = response.Headers["Content-Type"];
                Console.WriteLine("Object metadata, Title: {0}", title);
                Console.WriteLine("Content type: {0}", contentType);

                return reader.ReadToEnd(); // Now you process the response body.
            }
        }
        catch (Exception ex)
        {
            return ex.ToString();
        }
    }
    
    [HttpGet(Name = "Delete")]
    public async Task<string> Delete()
    {
        try
        {
            var client = GetS3Client();
    
            var deleteObjectRequest = new DeleteObjectRequest
            {
                BucketName = _s3Settings.BucketName,
                Key = FileKey
            };

            var response = await client.DeleteObjectAsync(deleteObjectRequest);

            return response.HttpStatusCode.ToString();
        }
        catch (Exception ex)
        {
            return ex.ToString();
        }
    }
}