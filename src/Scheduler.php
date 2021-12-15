<?php

namespace Makaryo\Scheduler;

class Scheduler
{
    private const MAX_EXECUTION_TIME = 600;

    private const TABLE_NAME = 'scheduler';

    protected \PDO $db;

    protected array $handlers = [];

    public function __construct($db)
    {
        $this->db = $db;
    }

    public function createSchedule(string $scheduleName, int $when, array $args = []): int
    {
        $statement = $this->db->prepare("INSERT INTO " . self::TABLE_NAME . "(task_name, status, schedule_time, args) VALUE(:task_name, :status, :schedule_time, :args)");
        $success = $statement->execute([
            ':task_name' => $scheduleName,
            ':status' => 'pending',
            ':schedule_time' => $when,
            ':args' => json_encode($args),
        ]);

        if ($success) {
            return $this->db->lastInsertId();
        }

        throw new \RuntimeException($statement->errorInfo()[2]);
    }

    public function on(string $scheduleName, $callback): void
    {
        $this->handlers[$scheduleName][] = $callback;
    }

    public function hasScheduled(string $scheduleName): bool
    {
        $statement = $this->db->prepare("SELECT COUNT(*) cnt FROM " . self::TABLE_NAME . " WHERE task_name = :task_name AND status IN ('pending', 'in-progress')");
        $statement->execute([':task_name' => $scheduleName]);
        $count = $statement->fetchColumn();
        $statement->closeCursor();

        return $count > 0;
    }

    public function run(): void
    {
        set_time_limit(self::MAX_EXECUTION_TIME);

        $this->retentionCleanUp();
        $this->releaseStalledTasks();

        $statement = $this->db->prepare("SELECT * FROM " . self::TABLE_NAME . " WHERE status = 'pending' AND schedule_time <= :schedule_time ORDER BY schedule_time ASC LIMIT 20");
        $statement->execute([':schedule_time' => time()]);
        $results = $statement->fetchAll(\PDO::FETCH_ASSOC);

        $ids = array_column($results, 'id');

        if (!empty($ids)) {
            $statement = $this->db->prepare("UPDATE " . self::TABLE_NAME . " SET status = 'in-progress', last_attempt = :last_attempt WHERE id IN (" . implode(',', $ids) . ")");
            $statement->execute([':last_attempt' => time()]);
        }

        foreach ($results as $result) {
            try {
                if (isset($this->handlers[$result['task_name']])) {
                    foreach ($this->handlers[$result['task_name']] as $handler) {
                        call_user_func_array($handler, json_decode($result['args'], true));
                    }
                }
                $this->markComplete($result['id']);
            } catch (\Throwable $e) {
                $this->markFailed($result['id'], $e->getMessage());
            }
        }
    }

    protected function markComplete(int $id): void
    {
        $statement = $this->db->prepare("UPDATE " . self::TABLE_NAME . " SET status = 'complete' WHERE id = :id");
        $statement->execute([':id' => $id]);
    }

    protected function markFailed(int $id, string $reason = ''): void
    {
        $statement = $this->db->prepare("UPDATE " . self::TABLE_NAME . " SET status = 'failed', info = :info WHERE id = :id");
        $statement->execute([':id' => $id, ':info' => $reason]);
    }

    protected function releaseStalledTasks(): void
    {
        $statement = $this->db->prepare("UPDATE " . self::TABLE_NAME . " SET status = 'failed', info = 'Time out.' WHERE last_attempt < :last_attempt AND status = 'in-progress'");
        $time = self::MAX_EXECUTION_TIME + 60;
        $statement->execute([':last_attempt' => strtotime("-{$time} seconds")]);
    }

    protected function retentionCleanUp(): void
    {
        $statement = $this->db->prepare("DELETE FROM " . self::TABLE_NAME . " WHERE last_attempt < :last_attempt");
        $statement->execute([':last_attempt' => strtotime("-30 days")]);
    }

    public function createDatabaseTable(): void
    {
        $tableName = self::TABLE_NAME;
        $sql = <<<SQL
            CREATE TABLE IF NOT EXISTS `$tableName` (
              `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
              `task_name` varchar(255) DEFAULT NULL,
              `status` enum('complete','pending','in-progress','failed','canceled') DEFAULT NULL,
              `schedule_time` int(10) unsigned DEFAULT NULL,
              `args` text DEFAULT NULL,
              `last_attempt` int(10) unsigned DEFAULT NULL,
              `info` text DEFAULT NULL,
              PRIMARY KEY (`id`)
            )
            SQL;
        $this->db->query($sql);
    }
}
