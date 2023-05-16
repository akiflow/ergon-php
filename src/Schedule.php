<?php
namespace Ergon;

use DateTime;

class Schedule {
    public ?string $id;
    public string $queue_id;
    public ?string $ordering_key;
    public string $subject;
    public int $every; // Minutes
    public ?DateTime $last_enqueued_at;
    public ?DateTime $next_enqueue_at;
    public int $max_enqueues;
    public ?int $total_enqueues;
    public int $max_retries;
    public array $payload;
    public int $ack_delay; //seconds

    public function toJSON(): array {
        return get_object_vars($this);
    }

    public static function fromJSON(string $data): Schedule {
        $json = json_decode($data, true);
        $sch = new Schedule();
        foreach ($json AS $key => $value) {
            $sch->{$key} = $value;
        }
        return $sch;
    }
}
