<?php
namespace Ergon;

use DateTime;
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

    public function toJSON(): array {
        $data = get_object_vars($this);
        foreach($data AS $key => $value) {
            if(is_a($value, 'DateTime')) {
                $data[$key] = $value->format(DateTime::ATOM);
            }
        }
        return $data;
    }

    public static function fromJSON(string $data): Job {
        $json = json_decode($data, true);
        $job = new Job();
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
}
