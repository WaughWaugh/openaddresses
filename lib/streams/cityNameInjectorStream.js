var through = require('through2');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parse/lib/sync');

var wofDictionary = {};

loadWofData();

function loadWofData() {
  var filePath = path.resolve(__dirname, `data/OA_Master_Data.csv`);

  if( fs.existsSync( filePath ) ) {
    const contents = fs.readFileSync(filePath,'UTF8');
    const lines = csv(contents, { columns: true });

    lines.forEach(p => wofDictionary[p.Name] = p );
  }
}


function create(){
  return through.obj(function(record, enc, next) {
    console.log("Running cityNameInjectorStream");
    
    console.log(JSON.stringify(record, null, 2));
 
    var recordPlaceholderInput = "";

    var aggregatedFilename = record.source_id.substring(0, record.source_id.indexOf(':'));
      
    if( aggregatedFilename in wofDictionary) {

      var wofData = wofDictionary[ aggregatedFilename ];

      if( wofData && wofData.WOFId && ( wofData.HasRegion || wofData.HasCity ) ) {
         record.setMeta('wofIdFromFileName', wofData.WOFId );
      }

      if( record.hasMeta( 'OACityRecord' ) ) {
         recordPlaceholderInput += record.getMeta( 'OACityRecord' ) + ",";
      }

      if( record.hasMeta( 'OARegionRecord' ) && wofData.Country ) {
         recordPlaceholderInput += record.getMeta( 'OARegionRecord' ) + "," +  wofData.Country;

      } else if( wofData && wofData.FQRegion ) {
         recordPlaceholderInput += wofData.FQRegion; 

      } else {
         recordPlaceholderInput = null;

      }

      if( recordPlaceholderInput != null ) {
         record.setMeta( 'recordPlaceholderInput', recordPlaceholderInput.toLowerCase() ); 
      }
    }

    next(null, record);
  });
};


module.exports.create = create;
