/* forkargs.c
 * Fork for each line of input, limiting parallelism to a specified
 * number of processes.
 * TODO: remote exec: parse slots, exec with ssh, set ssh params, set
 * pre- and post-commands for remote.
 * TODO: bail out on errors?
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>


/* Spawn off <n> jobs at a time.
 * Read command line arguments from stdin.
 * Use case is:
 *   find -name '*.tar' | forkargs bzip2 -9 
 * Input line is passed as a single argument to the command.
 *
 * We can pass that through to other commands by using sh:
 *
 * find . | forkargs sh -c 'cp $1 dest'
 */

FILE *trace = NULL;

typedef struct Slot Slot;
struct Slot
{
  char *hostname;
  pid_t cpid;
  char **args;
  int n_args;                   /* number of existing args. */
  char *arg;                    /* current argument */
};

Slot *slots;
int n_slots = 1;
const char *slots_string = NULL;

void print_slots(FILE *out)
{
  fprintf (out, "Slots:\n");
  if (slots)
    {
      int i;
      for (i = 0; i < n_slots; i++)
        {
          fprintf (out, "%20s %5d '%s'\n",
                   slots[i].hostname? slots[i].hostname : "(localhost)",
                   slots[i].cpid,
                   slots[i].cpid != -1? slots[i].arg : "-");
        }
    }
  else
    {
      fprintf (out, "(no slots)\n");
    }
}

void setup_slots(const char *str, char ** args, int n_args)
{
  int i;
  /* Initialise slots */
  slots = calloc (sizeof (Slot), n_slots);
  for (i = 0; i < n_slots; i++)
    {
      slots[i].hostname = NULL;
      slots[i].cpid = -1;
      slots[i].arg = NULL;
      slots[i].args = args;
      slots[i].n_args = n_args;
    }

  /* Parse the slots string and set up additional slots. */
  if (str)
    {
      const char *c = str;

      n_slots = 0;
      while (*c)
        {
          int num_slots = 1;
          char hostname[BUFSIZ];

          while (*c && isspace(*c))
            c++;

          /* int '*' hostname ? */
          if (*c && isdigit(*c))
            {
              const char *c2 = c;
              char num[BUFSIZ];
              i = 0;
              while (*c2 && isdigit(*c2))
                num[i++ % BUFSIZ] = *c2++;
              num[i++] = '\0';
              while (*c2 && isspace(*c2))
                c2++;
              if (*c2 && *c2 == '*')
                {
                  num_slots = atol(num);
                  c = c2+1;
                  while (*c && isspace(*c))
                    c++;
                }
            }

          /* Hostname */
          i = 0;
          while (*c && (isalnum(*c) || *c == '-' || *c == '.' || *c == '-' || *c == '@'))
            hostname[i++] = *c++;
          hostname[i++] = '\0';

          if (i == 1)
            {
              fprintf (stderr, "Bad hostname: '%s'\n", c);
              exit(2);
            }
          
          while (*c && isspace(*c))
            c++;

          for (i = 0; i < num_slots; i++)
            {
              int a, ai;
              char **slot_args;
              char *host = NULL;
              if (strcmp(hostname, "localhost") && strcmp(hostname, "-"))
                host = strdup(hostname);
              a = 0;
              slot_args = calloc (n_args + 2 + 2, sizeof(*slot_args));
              if (host)
                {
                  slot_args[a++] = "ssh";
                  slot_args[a++] = host;
                }
              for (ai = 0; ai < n_args; ai++)
                slot_args[a++] = args[ai];

              slots = realloc(slots, sizeof(*slots) * (++n_slots));
              slots[n_slots -1].hostname = host;
              slots[n_slots -1].cpid = -1;
              slots[n_slots -1].args = slot_args;
              slots[n_slots -1].n_args = a;
              slots[n_slots -1].arg = NULL;
            }
        }
    }
}

static char *
read_line_offset (FILE *in,
                  size_t offset)
{
  char buffer[BUFSIZ];
  char *result = NULL;
  if (!feof(stdin)
      && fgets(buffer, BUFSIZ, in))
    {
      size_t len = strlen(buffer);
      if (strstr(buffer, "\n") == NULL)
        {
          /* No newline in this, so get the rest of the string. */
          result = read_line_offset(in, offset + len);
        }
      else
        {
          /* We've read a newline. Allocate a buffer and set the
             terminating end of line character. */
          result = malloc (len + 1 + offset);
          result[len + offset] = '\0';
        }

      /* Copy the buffer into the result. */
      strncpy(&(result[offset]), buffer, len);
    }
  return result;
}


char *
read_line (FILE *in)
{
  return read_line_offset (in, 0);
}


void help()
{
  fprintf (stdout, "Syntax: forkargs -t<out> -j<n>\n");
  fprintf (stdout, " -t<out> trace process control info to <out>\n");
  fprintf (stdout, " -j<n>   Maximum of <n> parallel jobs\n");
}

void bad_arg(char *arg)
{
  fprintf (stderr, "Bad argument: '%s'\n", arg);
  help();
  exit (2);
}

void missing_arg(char *arg)
{
  fprintf (stderr, "Missing parameter to argument: '%s'\n", arg);
  help();
  exit (2);
}

/* Parse command-line arguments */
void parse_args(int argc, char *argv[], int *first_arg_p)
{
  int i;
  for (i = 1; i < argc && argv[i][0] == '-'; i++)
    {
      if (argv[i][1] == 'j')
        {
          if (argv[i][2] >= '0' && argv[i][2] <= '9')
            /* '-j<n>' */
            n_slots = atoi(&argv[i][2]);
          else if (argv[i][2])
            /* '-j<garbage>' */
            bad_arg(argv[i]);
          else if (i + 1 < argc)
            /* '-j' '<n>' */
            n_slots = atoi(argv[++i]);
          else
            missing_arg (argv[i]);
        }
      else if (argv[i][1] == 't')
        {
          const char *trace_name = "-";
          if (argv[i][2])
            /* '-t<filename>' */
            trace_name = &argv[i][2];
          else if (i + 1 < argc)
            /* '-t <filename>' */
            trace_name = argv[++i];
          else
            /* '-t' */
            missing_arg (argv[i]);
          if (trace_name[0] == '-' && trace_name[1] == '\0')
            trace = stderr;
          else
            trace = fopen (trace_name, "w");
          if (trace_name && !trace)
            {
              fprintf (stderr, "Cannot open trace file '%s'\n", trace_name);
              exit (0);
            }
        }
      else if (argv[i][1] == 's' && !strcmp(argv[i], "-slots"))
        {
          if (argv[i+1])
            slots_string = argv[++i];
          else
            missing_arg(argv[i]);
        }
      else if (argv[i][1] == 'h')
        {
          help();
          exit (0);
        }
      else
        bad_arg (argv[i]);
    }
  *first_arg_p = i;
}

int main (int argc, char *argv[])
{
  char *str;
  char **args;
  int first_arg;
  int line_arg;
  int n_active = 0;
  int cpid;
  int i;
  int slot;

  /* Defaults from environment */
  str = getenv("FORKARGS_J");
  if (str)
    n_slots = atoi (str);

  parse_args(argc, argv, &first_arg);

  if (n_slots <= 0)
    {
      fprintf (stderr, "Bad process limit (%d)\n", n_slots);
      exit (2);
    }

  /* Collect command arguments */
  args = calloc (argc - first_arg + 2, sizeof (char *));
  for (i = 0; i < argc - first_arg; i++)
    args[i] = argv[i + first_arg];
  line_arg = i;

  setup_slots (slots_string, args, line_arg);

  if (trace)
    print_slots(trace);

  while ((str = read_line (stdin)))
    {
      /* Strip newline */
      char *nl = strstr (str, "\n");
      if (nl)
        *nl = '\0';

      if (n_active >= n_slots)
        {
          int status;
          if (trace)
            fprintf (trace, "%s: %d processes active, waiting for"
                     " one to finish\n",
                     argv[0], n_active);
          /* Wait for one to exit before proceeding */
          cpid = wait (&status);
          if (cpid == -1)
            {
              perror(argv[0]);
              exit(1);
            }

          if (trace)
            fprintf (trace, "%s: child %d terminated\n", argv[0], cpid);

          /* Scan slot table and remove entry */
          for (i = 0; i < n_slots; i++)
            if (cpid == slots[i].cpid)
              {
                slots[i].cpid = -1;
                if (slots[i].arg)
                  free (slots[i].arg);
                break;
              }
          if (i == n_slots)
            {
              fprintf (stderr, "%s: cannot find child %d in slot table\n",
                       argv[0], cpid);
              exit(1);
            }

          n_active--;
        }

      /* Scan the slot table to find a free slot. */
      for (i = 0; i < n_slots; i++)
        if (slots[i].cpid == -1)
          break;
      if (i == n_slots)
        {
          fprintf (stderr, "%s: cannot find a free slot. Miscounted?\n",
                   argv[0]);
          exit(1);
        }
      slot = i;

      cpid = fork();
      if (cpid)
        {
          /* parent */
          slots[i].cpid = cpid;
          slots[i].arg = str;

          if (trace)
            {
              fprintf (trace, "Inserted in slot %d.\n", slot);
              print_slots(trace);
            }
          
          n_active++;
          if (trace)
            fprintf (trace, "%s: started child %d\n", argv[0], cpid);
        }
      else
        {
          /* Construct exec parameters */
          slots[slot].args[slots[slot].n_args] = str;
          slots[slot].args[slots[slot].n_args+1] = NULL;

          /* Child. Execute the process. */
          if (trace)
            {
              fprintf (trace, "%s: exec ", argv[0]);
              for (i = 0; i <= slots[slot].n_args; i++)
                fprintf (trace, "'%s' ", slots[slot].args[i]);
              fprintf (trace, "\n");
            }

          close(STDIN_FILENO);
          open("/dev/null", O_RDONLY);
          execvp(slots[slot].args[0], slots[slot].args);
          exit(0);
        }
    }

  /* Wait for all children to terminate */
  while (n_active)
    {
      int status;
      if (trace)
        fprintf (trace, "%s: waiting for %d children\n", argv[0], n_active);
      cpid = wait(&status);
      if (trace)
        fprintf (trace, "%s: child %d terminated\n", argv[0], cpid);
      n_active --;

      /* Clear out the slot table */
      for (i = 0; i < n_slots; i++)
        if (slots[i].cpid == cpid)
          {
            slots[i].cpid = -1;
            free (slots[i].arg);
            if (trace)
              {
                fprintf (trace, "Removed process from slot table entry %d\n", i);
                print_slots(trace);
                break;
              }
          }
    }

  return 0;
}
