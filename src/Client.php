<?php
namespace Ergon;

use DateTime;
use Exception;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\RequestException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

const DefaultScheduleMaxRetries = 10;
const DefaultClientMaxRetries = 3;
const DefaultRetryDelay = 100; // milliseconds

class Client {
    private \GuzzleHttp\Client $cli;
    private string $baseHost;
    private string $queueID;
    private string $token;
    private int $maxRetries;
    private int $retryDelay;

    /**
     * @throws Exception
     */
    public function __construct(
        string $baseHost,
        string $queueID,
        string $token,
        int $maxRetries = DefaultClientMaxRetries,
        int $retryDelay = DefaultRetryDelay  // in milliseconds
    ) {
        if (filter_var($baseHost, FILTER_VALIDATE_URL) === FALSE) {
            throw new Exception('INVALID_URL');
        }
        $array = array($baseHost, 'queues', $queueID);
        array_walk_recursive($array, function(&$component) {
            $component = rtrim($component, '/');
        });
        $url = implode('/', $array);
        $this->baseHost = $baseHost;
        $this->queueID = $queueID;
        $this->token = $token;
        $this->maxRetries = $maxRetries;
        $this->retryDelay = $retryDelay;

        // Create a handler stack with the retry middleware
        $handlerStack = HandlerStack::create();
        $handlerStack->push($this->createRetryMiddleware());

        $this->cli = new \GuzzleHttp\Client([
            'base_uri' => $url . '/',
            'timeout' => 3,
            'headers' => [
                'Authorization' => $token,
            ],
            'handler' => $handlerStack
        ]);
    }

    /**
     * Creates a retry middleware with exponential backoff
     *
     * @return callable
     */
    private function createRetryMiddleware(): callable
    {
        return Middleware::retry(
            // Retry condition
            function (
                $retries,
                RequestInterface $request,
                ?ResponseInterface $response = null,
                ?RequestException $exception = null
            ) {
                // Limit the number of retries
                if ($retries >= $this->maxRetries) {
                    return false;
                }

                // Retry on connection exceptions
                if ($exception instanceof ConnectException) {
                    return true;
                }

                // Retry on server errors (5xx)
                if ($response && $response->getStatusCode() >= 500) {
                    return true;
                }

                // Retry on rate limiting (429)
                if ($response && $response->getStatusCode() === 429) {
                    return true;
                }

                return false;
            },
            // Backoff strategy with exponential delay
            function ($retries) {
                // Calculate exponential backoff: baseDelay * 2^retries + small random jitter
                return $this->retryDelay * (2 ** $retries) + random_int(0, 100);
            }
        );
    }

    /**
     * @return Job[]
     * @throws Exception
     */
    function pull(int $count=1): array {
        $headers = [
            "Ergon-Batch-Size" => $count
        ];
        $resp = $this->cli->get('jobs/next',
        [
            'headers' => $headers
        ]);
        if ($resp->getStatusCode() == 204) {
            return [];
        }
        return Job::fromJSONArray($resp->getBody());
    }

    /**
     * @return Job[]
     * @throws Exception
     */
    function pullWait(int $wait, int $count=1) : array {
        $resp = $this->cli->get('jobs/wait', [
            'timeout' => ($wait+1),
            'headers' => [
                'Ergon-Wait' => $wait,
                'Ergon-Batch-Size' => $count
            ]
        ]);
        if ($resp->getStatusCode() == 204) {
            return [];
        }
        return Job::fromJSONArray($resp->getBody());
    }

    /**
     * @param Job[] $jobs
     * @throws Exception|GuzzleException
     */
    function push(array $jobs): void {
        $jsons = array();
        foreach($jobs AS $job) {
            $jsons[] = $job->toJSON();
        }
        $this->cli->post('jobs/batch', [
            'json' =>  $jsons
        ]);
    }

    /**
     * @throws Exception
     */
    function simplePush(?string $key, string $subject, DateTime $runAt, array $payload): ?Job
    {
        $resp = $this->cli->post('jobs', [
            "json" => $this->generateJob($key,$subject,$runAt,$payload)->toJSON()
        ]);
        return Job::fromJSON($resp->getBody());
    }

    function generateJob(?string $key, string $subject, DateTime $runAt, array $payload): Job {
        return new Job($this->queueID, $key, $subject, $runAt, $payload);
    }

    /**
     * @throws Exception
     */
    function ack(Job $job) {
        $this->cli->put('jobs/'.$job->id.'/ack', [
            'json' => $job->toJSON()
        ]);
    }

    /**
     * @throws Exception
     */
    function nack(Job $job) {
        $this->cli->put('jobs/'.$job->id.'/nack', [
            'json' => $job->toJSON()
        ]);
    }

    /**
     * @throws Exception
     */
    function nackWithDelay(Job $job, DateTime $when) {
        $job->retry_time = $when;
        $this->cli->put('jobs/'.$job->id.'/nack', [
            'json' => $job->toJSON()
        ]);
    }

    /**
     * @throws Exception
     */
    function term(Job $job) {
        $this->cli->put('jobs/'.$job->id.'/term', [
            'json' => $job->toJSON()
        ]);
    }

    /**
     * @throws Exception
     */
    function deleteJob(string $id) {
        $this->cli->delete('jobs/'.$id);
    }

    /**
     * @throws Exception
     */
    function deleteJobsWithKey(string $key) {
        $this->cli->delete('jobs/keys/'.$key);
    }

    /**
     * @throws Exception
     */
    function schedule(Schedule $sch): ?Schedule {
        $resp = $this->cli->post('schedules', [
            'json' => $sch->toJSON()
        ]);
        return Schedule::fromJSON($resp->getBody());
    }

    /**
     * @throws Exception
     */
    function simpleSchedule(?string $key, string $subject, int $minutes, array $payload): ?Schedule {
        $sch = new Schedule();
        $sch->id = null;
        $sch->queue_id = $this->queueID;
        $sch->ordering_key = $key;
        $sch->subject = $subject;
        $sch->every = $minutes;
        $sch->last_enqueued_at = null;
        $sch->next_enqueue_at = null;
        $sch->max_enqueues = 0;
        $sch->total_enqueues = 0;
        $sch->max_retries = DefaultScheduleMaxRetries;
        $sch->payload = $payload;
        $sch->ack_delay = 120;
        return $this->schedule($sch);
    }

    /**
     * @throws GuzzleException
     */
    function triggerSchedule(string $id) {
        $this->cli->post('schedules/'.$id.'/trigger');
    }

    /**
     * @throws GuzzleException
     */
    function deleteSchedule(string $id) {
        $this->cli->delete('schedules/'.$id);
    }

    /**
     * @throws GuzzleException
     */
    function ping(): bool {
        $resp = $this->cli->get($this->baseHost . '/ergon/system/healthz', ['timeout' => 1, 'http_errors' => false]);
        return $resp->getStatusCode() === 200;
    }
}
