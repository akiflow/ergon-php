<?php
namespace Ergon;

use Cassandra\Date;
use DateTime;

class Client {
    private \GuzzleHttp\Client $cli;
    private string $baseHost;
    private string $queueID;
    private string $token;

    /**
     * @throws \Exception
     */
    public function __construct(string $baseHost, string $queueID, string $token) {
        if (filter_var($baseHost, FILTER_VALIDATE_URL) === FALSE) {
            throw new \Exception('INVALID_URL');
        }
        $array = array($baseHost, 'queues', $queueID);
        array_walk_recursive($array, function(&$component) {
            $component = rtrim($component, '/');
        });
        $url = implode('/', $array);
        printf($url);
        $this->baseHost = $baseHost;
        $this->queueID = $queueID;
        $this->token = $token;
        $this->cli = new \GuzzleHttp\Client([
            'base_uri' => $url . '/',
            'timeout' => 11.0,
            'headers' => [
                'Authorization' => $token,
            ]
        ]);
    }

    /**
     * @throws \Exception
     */
    function pull() : ?Job {
        $resp = $this->cli->get('jobs/next');
        if ($resp->getStatusCode() == 204) {
            return null;
        }
        return Job::fromJSON($resp->getBody());
    }

    /**
     * @throws \Exception
     */
    function pullWait(int $wait) : ?Job {
        $resp = $this->cli->get('jobs/wait', [
            'timeout' => ($wait+1),
            'headers' => [
                'Ergon-Wait' => $wait,
            ]
        ]);
        if ($resp->getStatusCode() == 204) {
            return null;
        }
        return Job::fromJSON($resp->getBody());
    }

    /**
     * @throws \Exception
     */
    function push(Job $job) {
        $this->cli->post('jobs', [
            'json' => $job->toJSON()
        ]);
    }

    /**
     * @throws \Exception
     */
    function simplePush(?string $key, string $subject, DateTime $runAt, array $payload) {
        $job = new Job();
        $job->id = null;
        $job->queue_id = $this->queueID;
        $job->ordering_key = $key;
        $job->subject = $subject;
        $job->status = null;
        $job->enqueued_at = null;
        $job->run_at = $runAt;
        $job->pulled_at = null;
        $job->last_retry_at = null;
        $job->acked_at = null;
        $job->termed_at = null;
        $job->failed_at = null;
        $job->retry = 0;
        $job->max_retries = 50;
        $job->payload = $payload;
        $job->error = null;
        $job->ack_key = null;
        $job->retry_time = null;
        $job->ack_delay = 120;
        $job->expires_at = null;
        $this->push($job);
    }

    /**
     * @throws \Exception
     */
    function ack(Job $job) {
        $this->cli->put('jobs/'.$job->id.'/ack', [
            'json' => $job->toJSON()
        ]);
    }

    /**
     * @throws \Exception
     */
    function nack(Job $job) {
        $this->cli->put('jobs/'.$job->id.'/nack', [
            'json' => $job->toJSON()
        ]);
    }

    /**
     * @throws \Exception
     */
    function nackWithDelay(Job $job, DateTime $when) {
        $job->retry_time = $when;
        $this->cli->put('jobs/'.$job->id.'/nack', [
            'json' => $job->toJSON()
        ]);
    }

    /**
     * @throws \Exception
     */
    function term(Job $job) {
        $this->cli->put('jobs/'.$job->id.'/term', [
            'json' => $job->toJSON()
        ]);
    }

    /**
     * @throws \Exception
     */
    function deleteJobs(string $key) {
        $this->cli->delete('jobs/'.$key);
    }

    /**
     * @throws \Exception
     */
    function schedule(Schedule $sch) {
        $this->cli->post('schedules', [
            'json' => $sch->toJSON()
        ]);
    }

    /**
     * @throws \Exception
     */
    function simpleSchedule(?string $key, string $subject, int $minutes, array $payload) {
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
        $sch->max_retries = 50;
        $sch->payload = $payload;
        $sch->ack_delay = 120;
        $this->schedule($sch);
    }
}
