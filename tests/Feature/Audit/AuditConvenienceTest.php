<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Audit;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class AuditConvenienceTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();
        Schema::create('convenience_source', function ($table) {
            $table->id();
            $table->string('val')->nullable();
        });
    }

    private function assertRuleFails(string $rule, $valid, $invalid, array $args = [])
    {
        DB::table('convenience_source')->insert([['val' => $valid], ['val' => $invalid]]);

        $run = ExtractAndTransform::audit('convenience_source')
            ->identifiedBy('id')
            ->check(['val' => fn($r) => $r->{$rule}(...$args)])
            ->run();

        $this->assertEquals(1, $run->total_violations, "Rule '{$rule}' failed to find 1 violation.");
        $this->assertEquals('2', $run->logs->first()->row_identifier);

        DB::table('convenience_source')->truncate();
    }

    public function test_convenience_rules()
    {
        $this->assertRuleFails('email', 'test@example.com', 'not-an-email');
        $this->assertRuleFails('uuid', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'not-a-uuid');
        $this->assertRuleFails('url', 'http://example.com', 'not a url');
        $this->assertRuleFails('ip', '127.0.0.1', '999.999.999.999');
        $this->assertRuleFails('ipv4', '127.0.0.1', '2001:0db8:85a3:0000:0000:8a2e:0370:7334');
        $this->assertRuleFails('ipv6', '2001:0db8:85a3:0000:0000:8a2e:0370:7334', '127.0.0.1');
        $this->assertRuleFails('creditCard', '49927398716', '12345'); // Valid Luhn
        $this->assertRuleFails('isbn', '978-3-16-148410-0', '123-4-56-789012-3');
        $this->assertRuleFails('currencyCode', 'USD', 'USX');
        $this->assertRuleFails('latitude', '45.0', '91.0');
        $this->assertRuleFails('longitude', '-120.0', '-181.0');
        $this->assertRuleFails('timezone', 'UTC', 'Mars/Olympus_Mons');
        $this->assertRuleFails('alpha', 'abc', 'abc1');
        $this->assertRuleFails('alphaNum', 'abc1', 'abc-1');
        $this->assertRuleFails('alphaDash', 'abc-1_', 'abc 1');
        $this->assertRuleFails('json', '{"a":1}', '{"a":1');
    }

    public function test_rules_with_arguments()
    {
        // dateFormat
        $this->assertRuleFails('dateFormat', '2023-01-31', '2023-02-30', ['Y-m-d']);

        // startsWith (SQL)
        DB::table('convenience_source')->insert([['val' => 'START-ok'], ['val' => 'bad-START']]);
        $run = ExtractAndTransform::audit('convenience_source')->identifiedBy('id')->check(['val' => fn($r) => $r->startsWith('START')])->run();
        $this->assertEquals(1, $run->total_violations);
        DB::table('convenience_source')->truncate();

        // endsWith (SQL)
        DB::table('convenience_source')->insert([['val' => 'ok-END'], ['val' => 'END-bad']]);
        $run = ExtractAndTransform::audit('convenience_source')->identifiedBy('id')->check(['val' => fn($r) => $r->endsWith('END')])->run();
        $this->assertEquals(1, $run->total_violations);
        DB::table('convenience_source')->truncate();
    }
}
