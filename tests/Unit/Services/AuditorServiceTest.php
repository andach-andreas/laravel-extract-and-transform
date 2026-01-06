<?php

namespace Andach\ExtractAndTransform\Tests\Unit\Services;

use Andach\ExtractAndTransform\Models\AuditRun;
use Andach\ExtractAndTransform\Services\AuditorService;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class AuditorServiceTest extends TestCase
{
    use RefreshDatabase;

    private AuditorService $service;

    protected function setUp(): void
    {
        parent::setUp();
        $this->service = app(AuditorService::class);

        Schema::create('unit_audit_source', function ($table) {
            $table->id();
            $table->string('col1')->nullable();
        });
    }

    public function test_it_creates_audit_run_record()
    {
        $run = $this->service->run('unit_audit_source', 'id', []);

        $this->assertInstanceOf(AuditRun::class, $run);
        $this->assertTrue($run->exists);
        $this->assertEquals('unit_audit_source', $run->table_name);
        $this->assertEquals('success', $run->status);
    }

    public function test_it_counts_rows_scanned()
    {
        DB::table('unit_audit_source')->insert([
            ['col1' => 'A'],
            ['col1' => 'B'],
        ]);

        $run = $this->service->run('unit_audit_source', 'id', []);

        $this->assertEquals(2, $run->total_rows_scanned);
    }

    public function test_it_handles_composite_identifier()
    {
        // This tests the buildIdentifierSql logic indirectly via execution
        Schema::create('composite_source', function ($table) {
            $table->string('part1');
            $table->string('part2');
            $table->string('val')->nullable();
        });

        DB::table('composite_source')->insert([
            ['part1' => 'A', 'part2' => '1', 'val' => null],
        ]);

        $run = $this->service->run('composite_source', ['part1', 'part2'], [
            'val' => fn($r) => $r->required(),
        ]);

        $this->assertEquals(1, $run->total_violations);
        $log = $run->logs->first();

        // Expect "A-1" or similar depending on implementation
        // Implementation uses: implode(", '-', ", ...) -> CONCAT(`part1`, '-', `part2`)
        $this->assertEquals('A-1', $log->row_identifier);
    }
}
