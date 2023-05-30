<?php
namespace Ergon;

use DateTime;
use ReflectionException;
use ReflectionProperty;

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
        $data = get_object_vars($this);
        foreach($data AS $key => $value) {
            if(is_a($value, 'DateTime')) {
                $data[$key] = $value->format(DateTime::ATOM);
            }
        }
        return $data;
    }

    /**
     * @return Schedule
     * @throws ReflectionException
     */
    public static function fromJSON(string $data): Schedule {
        $json = json_decode($data, true);
        $sch = new Schedule();
        foreach ($json AS $key => $value) {
            $rp = new ReflectionProperty('\Ergon\Schedule', $key);
            if ($rp->getType()->getName() == 'DateTime') {
                $sch->{$key} = new DateTime($value);
            } else {
                $sch->{$key} = $value;
            }

        }
        return $sch;
    }
}
