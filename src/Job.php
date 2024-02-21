<?php
namespace Ergon;

use DateTime;
use DateTimeInterface;
use Exception;
use ReflectionException;
use ReflectionProperty;

class Job {
    public ?string $id;
    public string $queue_id;
    public ?string $ordering_key;
    public string $subject;
    public ?string $status;
    public ?DateTime $enqueued_at;
    public DateTime $run_at;
    public ?DateTime $pulled_at;
    public ?DateTime $last_retry_at;
    public ?DateTime $acked_at;
    public ?DateTime $termed_at;
    public ?DateTime $failed_at;
    public int $retry;
    public int $max_retries;
    public array $payload;
    public ?array $error;
    public ?string $ack_key;
    public ?DateTime $retry_time;
    public int $ack_delay; //seconds
    public ?DateTime $expires_at;

    public function __construct(string $queueID, ?string $key, string $subject, DateTime $runAt, array $payload) {
            $this->id = null;
            $this->queue_id = $queueID;
            $this->ordering_key = $key;
            $this->subject = $subject;
            $this->status = null;
            $this->enqueued_at = null;
            $this->run_at = $runAt;
            $this->pulled_at = null;
            $this->last_retry_at = null;
            $this->acked_at = null;
            $this->termed_at = null;
            $this->failed_at = null;
            $this->retry = 0;
            $this->max_retries = 50;
            $this->payload = $payload;
            $this->error = null;
            $this->ack_key = null;
            $this->retry_time = null;
            $this->ack_delay = 120;
            $this->expires_at = null;
    }

    public function toJSON(): array {
        $data = get_object_vars($this);
        foreach($data AS $key => $value) {
            if(is_a($value, 'DateTime')) {
                $data[$key] = $value->format(DateTimeInterface::ATOM);
            }
        }
        return $data;
    }

    /**
     * @return Schedule
     * @throws ReflectionException
     */
    public static function fromJSON(string $data): ?Job {
        if ($data == 'null') {
            return null;
        }
        $json = json_decode($data, true);
        $job = new Job($json['queue_id'], $json['ordering_key'] ?? null, $json['subject'], new DateTime($json['run_at']), $json['payload']);
        foreach ($json AS $key => $value) {
            $rp = new ReflectionProperty('\Ergon\Job', $key);
            if ($rp->getType()->getName() == 'DateTime') {
                $job->{$key} = new DateTime($value);
            } else {
                $job->{$key} = $value;
            }

        }
        return $job;
    }

    /**
     * @return Job[]
     * @throws ReflectionException | Exception
     */
    public static function fromJSONArray(string $data): array {
        $jsons = json_decode($data, true);
        $jobs = array();

        foreach($jsons AS $json) {
            $job = new Job($json['queue_id'], $json['ordering_key'] ?? null, $json['subject'], new DateTime($json['run_at']), $json['payload']);
            foreach ($json as $key => $value) {
                $rp = new ReflectionProperty('\Ergon\Job', $key);
                if ($rp->getType()->getName() == 'DateTime') {
                    $job->{$key} = new DateTime($value);
                } else {
                    $job->{$key} = $value;
                }
            }
            $jobs[] = $job;
        }
        return $jobs;
    }
}
