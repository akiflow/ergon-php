<?php
namespace Ergon;

use DateTime;

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
        return get_object_vars($this);
    }

    public static function fromJSON(string $data): Job {
        $json = json_decode($data, true);
        $job = new Job();
        foreach ($json AS $key => $value) {
            $job->{$key} = $value;
        }
        return $job;
    }
}
