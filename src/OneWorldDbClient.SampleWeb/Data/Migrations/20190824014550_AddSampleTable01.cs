using Microsoft.EntityFrameworkCore.Migrations;

namespace OneWorldDbClient.SampleWeb.Data.Migrations
{
    public partial class AddSampleTable01 : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "SampleTable01",
                columns: table => new
                {
                    SampleColumn01 = table.Column<string>(maxLength: 10, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_SampleTable01", x => x.SampleColumn01);
                });
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "SampleTable01");
        }
    }
}
