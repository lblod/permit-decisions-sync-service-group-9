import { app, errorHandler, sparqlEscapeUri, sparqlEscapeString, uuid } from "mu";
import { querySudo, updateSudo } from "@lblod/mu-auth-sudo";
import { CronJob } from "cron";

const ENDPOINT_PUBLICATION = process.env.ENDPOINT_PUBLICATION || "https://publicatie.hackathon-9.s.redhost.be/sparql";
const AUTO_SYNC = true;

function queryPublications(q) {
  let options = {};
  if (ENDPOINT_PUBLICATION) {
    options = {
      sparqlEndpoint: ENDPOINT_PUBLICATION,
      mayRetry: true,
    };
  }
  return querySudo(q, {}, options);
}

app.get("/start-sync", function (req, res) {
  startSync();
  res.send("started sync");
});

const syncJob = new CronJob("*/5 * * * *", async function () {
  startSync();
});

if (AUTO_SYNC) {
  syncJob.start();
}

async function startSync() {
  // get submissions from Loket that don't have a decision yet
  let q = `
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mu:   <http://mu.semte.ch/vocabularies/core/>
    PREFIX dbpedia: <http://dbpedia.org/resource/>
    PREFIX omgeving: <https://data.vlaanderen.be/ns/omgevingsvergunning#>

  SELECT DISTINCT ?submission WHERE {
  GRAPH ?g {
    ?case a dbpedia:Case ;
         omgeving:zaakhandeling ?submission  ;
         dct:subject ?event .
      ?submission a omgeving:Aanvraag ;
         omgeving:ingangsdatum ?submissionDate .
     ?event a omgeving:Activiteit .
     FILTER NOT EXISTS {
       ?decision a besluit:Besluit ;
         omgeving:voorwerp ?submission .
     }
  }
 } ORDER BY DESC(?submissionDate) LIMIT 100
  `;

  let result = await querySudo(q);
  console.log(`Found ${result.results.bindings.length} submissions for which we don't have a decision yet`);

  for (let binding of result.results.bindings) {
    console.log(`Try to fetch decision for submission <${binding['submission'].value}>`);
    // q = `
    //   PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    //   PREFIX omgeving: <https://data.vlaanderen.be/ns/omgevingsvergunning#>
    //   PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    //   SELECT ?decision ?decisionUuid
    //   {
    //     ?decision a besluit:Besluit ;
    //       mu:uuid ?decisionUuid ;
    //       omgeving:voorwerp ${sparqlEscapeUri(binding['submission'].value)} .
    //   } LIMIT 1
    // `;

    // Workaround: parse RDFa in snippet instead of querying for triples since extraction doesn't work yet
    const rdfaSnippet = `property="https://data.vlaanderen.be/ns/omgevingsvergunning#voorwerp" resource="${binding['submission'].value}"`;
    q = `
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
      PREFIX omgeving: <https://data.vlaanderen.be/ns/omgevingsvergunning#>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      SELECT ?decision ?decisionUuid
      {
        ?behandeling <http://www.w3.org/ns/prov#generated> ?decision .
        ?uittreksel <http://mu.semte.ch/vocabularies/ext/uittrekselBvap> ?behandeling ;
          <http://www.w3.org/ns/prov#value> ?inhoud .
        ?decision a besluit:Besluit ;
          mu:uuid ?decisionUuid .
        FILTER (CONTAINS(?inhoud, ${sparqlEscapeString(rdfaSnippet)}))
      } LIMIT 1
    `;

    result = await queryPublications(q);
    if (result.results.bindings.length) {
      const { decision, decisionUuid } = result.results.bindings[0];
      console.log(`Found decision ${decision.value}`);

      const permitUuid = uuid();
      const permitUri = `http://data.lblod.gift/id/vergunning/${permitUuid}`;
      const insertDecisionAndPermit = `
        PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
        PREFIX omgeving: <https://data.vlaanderen.be/ns/omgevingsvergunning#>
        PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
        INSERT {
          GRAPH ?g {
            ${sparqlEscapeUri(decision.value)} a besluit:Besluit ;
              mu:uuid ${sparqlEscapeString(decisionUuid.value)} ;
              omgeving:voorwerp ${sparqlEscapeUri(binding['submission'].value)} .
            ${sparqlEscapeUri(permitUri)} a omgeving:Vergunning ;
              mu:uuid ${sparqlEscapeString(permitUuid)} ;
              omgeving:inhoud ${sparqlEscapeUri(decision.value)} .
          }
        } WHERE {
          GRAPH ?g {
            ${sparqlEscapeUri(binding['submission'].value)} a omgeving:Aanvraag .
          }
        }
      `;
      await updateSudo(insertDecisionAndPermit);
      break;
    } else {
      console.log(`Could not find a decision for submission <${binding['submission'].value}>`);
    }
  }
}

app.use(errorHandler);
