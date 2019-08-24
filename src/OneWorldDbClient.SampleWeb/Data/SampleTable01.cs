using System.ComponentModel.DataAnnotations;

namespace OneWorldDbClient.SampleWeb.Data
{
    public partial class SampleTable01
    {
        [Key]
        [StringLength(10)]
        public string SampleColumn01 { get; set; }
    }
}
