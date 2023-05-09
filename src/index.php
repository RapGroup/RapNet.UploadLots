<?php 

namespace Rapnet\RapnetUploadLots;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\ClientInterface;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Promise\Utils;
use GuzzleHttp\HandlerStack;
use GuzzleRetry\GuzzleRetryMiddleware;
use React\EventLoop\Loop;

require('env.php');

class MultipartStartRequest {
    public $fileName;
    public $replaceAll;
    public $sendEmail;
    public $diamondFileFormat;
    public $fileSize;
}

class LotsRequest {
    public $clientRowIds;    
    public $stockNumbers;
}
class DeleteLotsRequest extends LotsRequest {
    public $rapnetLotIds;
}

class Index {

    private $config;

    public function __construct($clientId = null, $clientSecret = null) {
        $this->config = 
            [
              'base_path' => $_ENV['RAPNET_GATEWAY_BASE_URL'],
              'authorization_url' => $_ENV['RAPNET_AUTH_URL'],
              'machine_auth_url' => $_ENV['RAPNET_MACHINE_TO_MACHINE_AUTH_URL'],
              'client_id' => $clientId,
              'client_secret' => $clientSecret,
              'redirect_uri' => null,              
              'token_callback' => null,
              'diamondupdateingest' => $_ENV['RAPNET_GATEWAY_BASE_URL'].'/diamondupdateingest/api/public',
              'diamondupdate' => $_ENV['RAPNET_GATEWAY_BASE_URL'].'/diamondupdate/api/public',
              'jwt' => null,
              'scope' => 'manageListings priceListWeekly instantInventory',
              'audience' => $_ENV['RAPNET_GATEWAY_AUDIENCE']
            ];
    }

    /*
    * Get Auth Token
    *
    * @return { access_token: string, token_type: 'Bearer' }
    *
    *
    */

    public function getAuthorizationToken()
    {
        try {
            $stack = HandlerStack::create();
            $stack->push(GuzzleRetryMiddleware::factory([
                'max_retry_attempts' => 2,
                'retry_on_status' => [429, 503, 500]
            ]));

            $client = new GuzzleClient(['verify' => false, 'handler' => $stack]);
            $url = "{$this->config['machine_auth_url']}/api/get";

            $response = $client->request('GET',  $url, [
                    'headers' => [
                        'Content-Type' => 'application/json',
                    ],
                    'json' => [
                        'client_id' => $this->config['client_id'],
                        'client_secret' => $this->config['client_secret']
                    ],
                ]
            );
            return json_decode($response->getBody());
        } catch (RequestException $e) {
            return $e->getMessage();
        }        
    }

    private function splitFileToChunks($fileName, $indx, $chunkSize=1024) {        
        $sentSize = $indx * $chunkSize;
        $chunk = file_get_contents($fileName, FALSE, NULL, $sentSize, $chunkSize);

        return $chunk;
    }

    /*
    * Upload Status
    *
    * @param token string
    * @param s3UploadId string
    * @param rapnetLotIds string[]
    * @param stockNumbers string[]
    *
    * @return string or 
    *    {
    *        "uploadID": 0,
    *        "uploadType": "RAPNET_COM",
    *        "fileFormat": "Rapnet",
    *        "stockReplaced": true,
    *        "dateUploaded": "2023-04-12T08:42:29.163Z",
    *        "status": "NewIn",
    *        "errorMessages": "string",
    *        "warningMessages": "string",
    *        "numLotReceived": 0,
    *        "numValidLots": 0,
    *        "numInvalidLots": 0,
    *        "startTime": "2023-04-12T08:42:29.163Z",
    *        "endTime": "2023-04-12T08:42:29.163Z",
    *        "lastUpdated": "2023-04-12T08:42:29.163Z",
    *        "duration": "string",
    *        "progressPercent": 0,
    *        "waitingINQueue": 0
    *    }
    *
    */

    public function getUploadStatus($token, $s3UploadId)
    {
        try {
            $stack = HandlerStack::create();
            $stack->push(GuzzleRetryMiddleware::factory([
                'max_retry_attempts' => 2,
                'retry_on_status' => [429, 503, 500]
            ]));

            $client = new GuzzleClient(['verify' => false, 'handler' => $stack]);
            $url = "{$this->config['diamondupdateingest']}/lots/status/{$s3UploadId}";

            $response = $client->request('GET',  $url, [
                    'headers' => [
                        'Content-Type' => 'application/json',
                        'authorization' => "Bearer {$token}"
                    ]
                ]
            );
            return json_decode($response->getBody());
        } catch (RequestException $e) {
            return $e->getMessage();
        }    
    }


    /*
    * Upload CSV File
    *
    * @param token string
    * @param file FILE
    * @param diamondFileFormat string (ex. Rapnet)
    * @param replace boolean
    * @param sendEmail boolean
    * @param waitForResult boolean (optional)
    * @param interval number (optional in seconds)
    *
    * @return string (uploadId)
    *
    */

    public function uploadFile($token, $file, $diamondFileFormat, $replace, $sendEmail, $waitForResult = false, $interval = 60.0)
    {
        try {
            $stack = HandlerStack::create();
            $stack->push(GuzzleRetryMiddleware::factory([
                'max_retry_attempts' => 2,
                'retry_on_status' => [429, 503, 500]
            ]));
          
            $client = new GuzzleClient([
                'verify' => false, 
                'handler' => $stack
            ]);
            
            $url = "{$this->config['diamondupdateingest']}/lots/multipart";

            $allowed = array('csv', 'txt');
            $mime_types  = array('txt' => 'text/plain', 'csv' => 'text/csv');

            $ext = pathinfo($file['name'], PATHINFO_EXTENSION);

            if (!in_array($ext, $allowed)) {
               return 'Support only .csv and .txt files';
            }

            $headers = ['Content-Type' => 'application/json', 'authorization' => "Bearer {$token}"];
            
            $start_body = new MultipartStartRequest();
            $start_body->fileName = $file['name'];
            $start_body->replaceAll = $replace;
            $start_body->sendEmail = $sendEmail;
            $start_body->diamondFileFormat = $diamondFileFormat;
            $start_body->fileSize = $file['size'];

            $start_request = new Request('POST', $url, $headers, json_encode($start_body));

            $finalPromise = $client->sendAsync($start_request)->then(function ($response1) use (&$client, &$ext, &$file, &$mime_types, &$token, &$waitForResult, &$interval) {          
                $multipart_body = json_decode($response1->getBody());

                $promises = [];

                foreach ($multipart_body->putUrls as $key => $putUrl) {
                    $chunk = $this->splitFileToChunks($file['tmp_name'], $key, $multipart_body->multipartSplitSize);
                    $promises[] =  $client->requestAsync('PUT', $putUrl, [
                        'headers' => [
                            'Content-Type' => $mime_types[$ext],
                        ],
                        'body' => $chunk
                    ]);
                }
                // $responses = Promise\Utils::unwrap($promises);
                $responses = Utils::settle($promises)->wait();
                $etags = [];

                foreach ($multipart_body->putUrls as $key => $putUrl) {
                    $etags[$key + 1] = $responses[$key]['value']->getHeader('ETag')[0];
                }

                $multipart_finish_url = "{$this->config['diamondupdateingest']}/lots/multipart";
                $multipart_finish_obj = [
                    "fileName" => $file['name'],
                    "uploadId" => $multipart_body->uploadId,
                    "multipartUploadId" => $multipart_body->multipartUploadId,
                    "eTags" => $etags
                ];

                $multipart_finish_request = $client->requestAsync('PUT', $multipart_finish_url, [
                    'headers' => [
                        'Content-Type' => 'application/json',
                        'authorization' => "Bearer {$token}"
                    ],
                    'json' => $multipart_finish_obj
                ]);

                $multipart_finish_response = $multipart_finish_request->wait();
                $multipart_finish_status = $multipart_finish_response->getStatusCode();

                $s3UploadId = $multipart_body->uploadId;

                if ($multipart_finish_status === 200) {
                    if ($waitForResult) {
                        $loop = Loop::get();
                        $upload_status_result = [];

                        $loop->addPeriodicTimer($interval, function ($timer) use ($loop, &$s3UploadId, &$token, &$client, &$upload_status_result) {
                            $upload_status_url = "{$this->config['diamondupdateingest']}/lots/status/{$s3UploadId}";
                            $upload_status_request = $client->requestAsync('GET', $upload_status_url, [
                                'headers' => [
                                    'Content-Type' => 'application/json',
                                    'authorization' => "Bearer {$token}"
                                ]
                            ]);            
                            $upload_status_response = $upload_status_request->wait();                            
                            $upload_status_body = json_decode($upload_status_response->getBody());

                            if ($upload_status_body->status === 'Finished successfully' || $upload_status_body->status === 'Failed' || $upload_status_body->status === 'S3UploadId wasnt found in s3') {
                                $upload_status_result = $upload_status_body;
                                $loop->cancelTimer($timer);
                            }
                        });
                        $loop->run();
                        return $upload_status_result;
                    }
                    return $s3UploadId;
                }
                return $multipart_finish_response;
            });

            return $finalPromise->wait();
        } catch (RequestException $e) {
            return $e->getMessage();
        }        
    }

    /*
    * Delete lots
    *
    * @param token string
    * @param clientRowIds string[]
    * @param rapnetLotIds string[]
    * @param stockNumbers string[]
    *
    * @return string or 
    *  {
    *    "notFound": {
    *      "clientRowIds": [
    *        "string"
    *      ],
    *      "rapnetLotIds": [
    *        "string"
    *      ],
    *      "stockNumbers": [
    *        "string"
    *      ]
    *    },
    *    "totalDeleted": 0
    *  }
    *
    */

    public function deleteLots($token, $clientRowIds = [], $rapnetLotIds = [], $stockNumbers = [])
    {
        try {
            $stack = HandlerStack::create();
            $stack->push(GuzzleRetryMiddleware::factory([
                'max_retry_attempts' => 2,
                'retry_on_status' => [429, 503, 500]
            ]));

            $client = new GuzzleClient(['verify' => false, 'handler' => $stack]);
            $url = "{$this->config['diamondupdate']}/lots";

            $delete_lots_body = new DeleteLotsRequest();
            $delete_lots_body->clientRowIds = $clientRowIds;
            $delete_lots_body->rapnetLotIds = $rapnetLotIds;
            $delete_lots_body->stockNumbers = $stockNumbers;
            

            $response = $client->request('DELETE',  $url, [
                    'headers' => [
                        'Content-Type' => 'application/json',
                        'authorization' => "Bearer {$token}"
                    ],
                    'body' => json_encode($delete_lots_body)
                ]
            );
            return json_decode($response->getBody());
        } catch (RequestException $e) {
            return $e->getMessage();
        }    
    }

    /*
    * Keep Alive Lots
    *
    * @param token string
    * @param clientRowIds string[]    
    * @param stockNumbers string[]
    *
    * @return string or 
    *  {
    *    "notFound": {
    *      "clientRowIds": [
    *        "string"
    *      ],
    *      "stockNumbers": [
    *        "string"
    *      ]
    *    },
    *    "totalUpdated": 0
    *  }
    *
    */

    public function keepAliveLots($token, $clientRowIds = [], $stockNumbers = [])
    {
        try {
            $stack = HandlerStack::create();
            $stack->push(GuzzleRetryMiddleware::factory([
                'max_retry_attempts' => 2,
                'retry_on_status' => [429, 503, 500]
            ]));

            $client = new GuzzleClient(['verify' => false, 'handler' => $stack]);
            $url = "{$this->config['diamondupdate']}/lots/keepalive";

            $lots_body = new LotsRequest();
            $lots_body->clientRowIds = $clientRowIds;
            $lots_body->stockNumbers = $stockNumbers;
            

            $response = $client->request('PUT',  $url, [
                    'headers' => [
                        'Content-Type' => 'application/json',
                        'authorization' => "Bearer {$token}"
                    ],
                    'body' => json_encode($lots_body)
                ]
            );
            return json_decode($response->getBody());
        } catch (RequestException $e) {
            return $e->getMessage();
        }    
    }

    /*
    * Keep Alive All Lots
    *
    * @param token string
    *
    * @return string or 
    *  {
    *    "totalUpdated": 0
    *  }
    *
    */
    public function keepAliveAll($token)
    {
        try {
            $stack = HandlerStack::create();
            $stack->push(GuzzleRetryMiddleware::factory([
                'max_retry_attempts' => 2,
                'retry_on_status' => [429, 503, 500]
            ]));

            $client = new GuzzleClient(['verify' => false, 'handler' => $stack]);
            $url = "{$this->config['diamondupdate']}/lots/keepalive/all";
            

            $response = $client->request('PUT',  $url, [
                    'headers' => [
                        'Content-Type' => 'application/json',
                        'authorization' => "Bearer {$token}"
                    ]
                ]
            );
            return json_decode($response->getBody());
        } catch (RequestException $e) {
            return $e->getMessage();
        }    
    }
}